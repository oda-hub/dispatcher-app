#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May 10 10:55:20 2017

@author: andrea tramcere
"""

from builtins import (open, str, range,
                      object)

import traceback

from collections import Counter, OrderedDict
import copy

import logging
from werkzeug.utils import secure_filename

import os
import glob
import string
import random
from raven.contrib.flask import Sentry

from flask import jsonify, send_from_directory, redirect
from flask import Flask, request, g
from urllib.parse import urlencode
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import email
import time as time_

import tempfile
import tarfile
import gzip
import socket
import logstash
import hashlib
import typing
import jwt
import smtplib
import ssl

from ..plugins import importer
from ..analysis.queries import * # TODO: evil wildcard import
from ..analysis import tokenHelper
from ..analysis.job_manager import Job, job_factory
from ..analysis.io_helper import FilePath
from .mock_data_server import mock_query
from ..analysis.products import QueryOutput
from ..configurer import DataServerConf
from ..analysis.plot_tools import Image
from ..analysis.exceptions import BadRequest, APIerror, MissingParameter, RequestNotUnderstood, RequestNotAuthorized, ProblemDecodingStoredQueryOut
from . import tasks
from oda_api.data_products import NumpyDataProduct
import oda_api

logger = logging.getLogger(__name__)


class NoInstrumentSpecified(BadRequest):
    pass


class InstrumentNotRecognized(BadRequest):
    pass


class MissingRequestParameter(BadRequest):
    pass


class EMailNotSent(BadRequest):
    pass


class InstrumentQueryBackEnd:

    def __repr__(self):
        return f"[ {self.__class__.__name__} : {self.instrument_name} ]"

    @property
    def instrument_name(self):
        return getattr(self, '_instrument_name', 'instrument-not-set')

    @instrument_name.setter
    def instrument_name(self, instrument_name):
        self._instrument_name = instrument_name

    def __init__(self, app,
                 instrument_name=None,
                 par_dic=None,
                 config=None,
                 data_server_call_back=False,
                 verbose=False,
                 get_meta_data=False):

        self.logger = logging.getLogger(__name__)

        if verbose:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        self.app = app
        try:
            if par_dic is None:
                self.set_args(request, verbose=verbose)
            else:
                self.par_dic = par_dic

            self.client_name = self.par_dic.pop('client-name', 'unknown')
            if os.environ.get("DISPATCHER_ASYNC_ENABLED", "no") == "yes":  # TODO: move to config!
                self.async_dispatcher = self.par_dic.pop(
                    'async_dispatcher', 'True') == 'True'  # why string true?? else false anyway
            else:
                self.async_dispatcher = False
            """
                async dispatcher operation avoids building QueryOutput in the sync request, and instead sends it in the queue
                in the queue, the same request is repeated, same session id/job id, but requesting sync request
                this imitates two repeated identical requests from the same client, which takes care of aliasing/etc complexity
                the remaining complexity is to send back a response which indicates "submitted" but not submitted job - only request
            """

            self.set_session_id()

            self.time_request = None
            if 'time_request' in self.par_dic:
                self.time_request = float(self.par_dic['time_request'])
                self.par_dic.pop('time_request')
            else:
                self.time_request = g.get('request_start_time', None)

            # By default, a request is public, let's now check if a token has been included
            # In that case, validation is needed
            self.public = True
            self.token = None
            self.decoded_token = None
            if 'token' in self.par_dic.keys() and self.par_dic['token'] != "":
                self.token = self.par_dic['token']
                self.public = False
                # token validation and decoding can be done here, to check if the token is expired
                try:
                    if self.validate_query_from_token():
                        pass
                except jwt.exceptions.ExpiredSignatureError as e:
                    raise RequestNotAuthorized("token expired")

            if get_meta_data:
                self.logger.info("get_meta_data request")
                self.set_instrument(instrument_name,)
                # this assumption might be completely wrong to be confirmed
                self.job_id = None
                # decide if it is worth to add the logger also in this case
                self.set_scratch_dir(self.par_dic['session_id'], verbose=verbose)
                self.set_session_logger(self.scratch_dir, verbose=verbose, config=config)
                self.set_sentry_client()
            else:
                self.logger.info("NOT get_meta_data request: yes scratch_dir")

                # TODO: if not callback!
                # if 'query_status' not in self.par_dic:
                #    raise MissingRequestParameter('no query_status!')

                if data_server_call_back is True:
                    self.job_id = None
                    if 'job_id' in self.par_dic:
                        self.job_id = self.par_dic['job_id']

                else:
                    query_status = self.par_dic['query_status']
                    self.job_id = None
                    if query_status == 'new':
                        self.generate_job_id()
                    else:
                        if 'job_id' not in self.par_dic:
                            raise RequestNotUnderstood(
                                f"job_id must be present if query_status != \"new\" (it is \"{query_status}\")")

                        self.job_id = self.par_dic['job_id']

                self.set_scratch_dir(
                    self.par_dic['session_id'], job_id=self.job_id, verbose=verbose)

                self.set_session_logger(self.scratch_dir, verbose=verbose, config=config)
                self.set_sentry_client()

                self.set_instrument(instrument_name, data_server_call_back)

                self.config = config

            self.logger.info(f'==> found par dict {self.par_dic.keys()}')

        except APIerror:
            raise

        except Exception as e:
            self.logger.error(
                '\033[31mexception in constructor of %s %s\033[0m', self, repr(e))
            self.logger.error("traceback: %s", traceback.format_exc())

            query_out = QueryOutput()
            query_out.set_query_exception(
                e, 'InstrumentQueryBackEnd constructor', extra_message='InstrumentQueryBackEnd constructor failed')

            #out_dict = {}
            #out_dict['query_status'] = 1
            #out_dict['exit_status'] = query_out.status_dictionary
            self.build_dispatcher_response(
                query_new_status='failed', query_out=query_out)

            # return jsonify(out_dict)

    def make_hash(self, o):
        """
        Makes a hash from a dictionary, list, tuple or set to any level, that contains
        only other hashable types (including any lists, tuples, sets, and
        dictionaries).

        """

        # note that even strings change hash() value between python invocations, so it's not safe to do so
        def format_hash(x): return hashlib.md5(
            json.dumps(sorted(x)).encode()
        ).hexdigest()[:16]

        if isinstance(o, (set, tuple, list)):
            return format_hash(tuple(map(self.make_hash, o)))

        elif isinstance(o, (dict, OrderedDict)):
            return self.make_hash(tuple(o.items()))

        # this takes care of various strange objects which can not be properly represented
        return format_hash(json.dumps(o))

    # not job_id??
    def generate_job_id(self, kw_black_list=['session_id', 'job_id', 'token']):
        self.logger.info("\033[31m---> GENERATING JOB ID <---\033[0m")
        self.logger.info(
            "\033[31m---> new job id for %s <---\033[0m", self.par_dic)

        # TODO generate hash (immutable ore convert to Ordered): DONE
        #import collections

        # self.par_dic-> collections.OrderedDict(self.par_dic)
        # oredered_dict=OrderedDict(self.par_dic)

        #self.job_id=u''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))

        _dict = OrderedDict({
            k: v for k, v in self.par_dic.items()
            if k not in kw_black_list
        })
        if not self.public:
            # token has not been considered, but the user id will be (if availaable)
            _dict['sub'] = tokenHelper.get_token_user_email_address(self.decoded_token)

        self.job_id = u'%s' % (self.make_hash(_dict))

        self.logger.info(
            '\033[31mgenerated NEW job_id %s \033[0m', self.job_id)

    def set_session_id(self):
        self.logger.info("---> SET_SESSION_ID <---")
        if 'session_id' not in self.par_dic.keys():
            self.par_dic['session_id'] = None

        self.logger.info('passed SESSION ID: %s', self.par_dic['session_id'])

        if self.par_dic['session_id'] is None or self.par_dic['session_id'] == 'new':
            self.logger.info('generating SESSION ID: %s',
                             self.par_dic['session_id'])
            self.par_dic['session_id'] = u''.join(random.choice(
                string.ascii_uppercase + string.digits) for _ in range(16))

        self.logger.info('setting SESSION ID: %s', self.par_dic['session_id'])

    def set_session_logger(self, scratch_dir, verbose=False, config=None):
        self.logger = logging.getLogger(repr(self))

        if verbose:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        session_log_filename = os.path.join(scratch_dir, 'session.log')

        have_handler = False
        for handler in self.logger.handlers:
            if isinstance(handler, logging.FileHandler):
                self.logger.info("found FileHandler: %s : %s", handler, handler.baseFilename)
                have_handler = True
                #handler.baseFilename == session_log_filename

        if not have_handler:
            fileh = logging.FileHandler(session_log_filename, 'a')
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fileh.setFormatter(formatter)
            self.logger.addHandler(fileh)  # set the new handler

        if verbose:
            self.logger.info(f'logfile set to dir= {scratch_dir}, with name= {session_log_filename}')

        # if config is not None:
        #    logger=self.set_logstash(logger,logstash_host=config.logstash_host,logstash_port=config.logstash_port)

    def set_logstash(self, logger, logstash_host=None, logstash_port=None):
        _logger = logger
        if logstash_host is not None:
            logger.addHandler(logstash.TCPLogstashHandler(
                logstash_host, logstash_port))

            extra = {
                'origin': 'cdci_dispatcher',
            }
            _logger = logging.LoggerAdapter(logger, extra)
        else:
            pass

        return _logger

    def set_sentry_client(self, sentry_url=None):

        if sentry_url is not None:
            from raven import Client

            client = Client(sentry_url)
        else:
            client = None

        self.sentry_client = client

    def get_current_ip(self):
        return socket.gethostbyname(socket.gethostname())

    def set_args(self, request, verbose=False):
        if request.method == 'GET':
            args = request.args
        if request.method == 'POST':
            args = request.form
        self.par_dic = args.to_dict()

        if verbose:
            self.logger.info(f'par_dic {self.par_dic}')

        if 'scw_list' in self.par_dic.keys():
            _p = request.args.getlist('scw_list')
            if len(_p) > 1:
                self.par_dic['scw_list'] = _p
            print('=======> scw_list',  self.par_dic['scw_list'], _p, len(_p))

        self.args = args

    def set_scratch_dir(self, session_id, job_id=None, verbose=False):
        if verbose:
            print('SETSCRATCH  ---->', session_id,
                  type(session_id), job_id, type(job_id))

        wd = 'scratch'

        if session_id is not None:
            wd += '_sid_' + session_id

        if job_id is not None:
            wd += '_jid_'+job_id

        alias_workdir = self.get_existing_job_ID_path(
            wd=FilePath(file_dir=wd).path)
        if alias_workdir is not None:
            wd = wd+'_aliased'

        wd = FilePath(file_dir=wd)
        wd.mkdir()
        self.scratch_dir = wd.path

    def prepare_download(self, file_list, file_name, scratch_dir):

        file_name = file_name.replace(' ', '_')

        if hasattr(file_list, '__iter__'):
            print('file_list is iterable')
        else:
            file_list = [file_list]

        for ID, f in enumerate(file_list):
            file_list[ID] = os.path.join(scratch_dir + '/', f)

        tmp_dir = tempfile.mkdtemp(prefix='download_', dir='./')
        #print('using tmp dir', tmp_dir)

        file_path = os.path.join(tmp_dir, file_name)
        #print('writing to file path', file_path)
        out_dir = file_name.replace('.tar', '')
        out_dir = out_dir.replace('.gz', '')

        if len(file_list) > 1:
            #print('preparing tar')
            tar = tarfile.open("%s" % (file_path), "w:gz")
            for name in file_list:
                #print('add to tar', file_name,name)
                if name is not None:
                    tar.add(name, arcname='%s/%s' %
                            (out_dir, os.path.basename(name)))
            tar.close()
        else:
            #print('single fits file')
            in_data = open(file_list[0], "rb").read()
            with gzip.open(file_path, 'wb') as f:
                f.write(in_data)

        tmp_dir = os.path.abspath(tmp_dir)

        return tmp_dir, file_name

    def download_products(self,):
        #print('in url file_list', self.args.get('file_list'))
        #scratch_dir, logger = set_session(self.args.get('session_id'))

        file_list = self.args.get('file_list').split(',')
        #print('used file_list', file_list)
        file_name = self.args.get('download_file_name')

        tmp_dir, target_file = self.prepare_download(
            file_list, file_name, self.scratch_dir)
        #print('downlaoding scratch dir', self.scratch_dir)
        try:
            return send_from_directory(directory=tmp_dir, filename=target_file, attachment_filename=target_file,
                                       as_attachment=True)
        except Exception as e:
            return e

    def upload_file(self, name, scratch_dir):
        #print('upload  file')
        #print('name', name)
        #print('request.files ',request.files)
        if name not in request.files:
            return None
        else:
            file = request.files[name]
            #print('type file', type(file))
            # if user does not select file, browser also
            # submit a empty part without filename
            if file.filename == '' or file.filename is None:
                return None

            filename = secure_filename(file.filename)
            # print('scratch_dir',scratch_dir)
            #print('secure_file_name', filename)
            file_path = os.path.join(scratch_dir, filename)
            file.save(file_path)
            # return redirect(url_for('uploaded_file',
            #                        filename=filename))
            return file_path

    def get_meta_data(self, meta_name=None):
        src_query = SourceQuery('src_query')

        l = []
        if meta_name is None:
            if 'product_type' in self.par_dic.keys():
                prod_name = self.par_dic['product_type']
            else:
                prod_name = None
            if hasattr(self, 'instrument'):
                l.append(self.instrument.get_parameters_list_as_json(
                    prod_name=prod_name))
                src_query.show_parameters_list(self.logger)
            else:
                l = ['instrument not recognized']

        if meta_name == 'src_query':
            l = [src_query.get_parameters_list_as_json()]
            src_query.show_parameters_list(self.logger)

        if meta_name == 'instrument':
            l = [self.instrument.get_parameters_list_as_json()]
            self.instrument.show_parameters_list(self.logger)

        return jsonify(l)

    def get_api_par_names(self):
        _l = []
        if 'product_type' in self.par_dic.keys():
            prod_name = self.par_dic['product_type']
        else:
            prod_name = None
        if hasattr(self, 'instrument'):
            _l = self.instrument.get_parameters_name_list(prod_name=prod_name)
            if 'user_catalog' in _l:
                _l.remove('user_catalog')
        else:
            _l = ['instrument not recognized']
        return jsonify(_l)

    def get_paramters_dict(self):
        return jsonify(self.par_dic)

    def get_instr_list(self, name=None):
        _l = []
        for instrument_factory in importer.instrument_factory_list:
            _l.append(instrument_factory().name)

        return jsonify(_l)

    @property
    def dispatcher_callback_url_base(self):
        return getattr(self, '_dispatcher_callback_url_base',
                       getattr(self.config, 'dispatcher_callback_url_base', None))

    @property
    def dispatcher_host(self):
        return getattr(self, '_dispatcher_host',
                       getattr(self.config, 'bind_host', None))

    @property
    def dispatcher_port(self):
        return getattr(self, '_dispatcher_port',
                       getattr(self.config, 'bind_port', None))

    def run_call_back(self, status_kw_name='action'):

        try:
            config, self.config_data_server = self.set_config()
        except Exception as e:
            query_out = QueryOutput()
            query_out.set_query_exception(e, 'run_call_back failed in %s' % self.__class__.__name__,
                                          extra_message='configuration failed')

        self.config, self.config_data_server = self.set_config()
        if self.config.sentry_url is not None:
            self.set_sentry_client(self.config.sentry_url)
        session_id = self.par_dic['session_id']
        instrument_name = self.par_dic.get('instrument_name', '')
        job = job_factory(instrument_name,
                          self.scratch_dir,
                          self.dispatcher_host,
                          self.dispatcher_port,
                          self.dispatcher_callback_url_base,
                          self.par_dic['session_id'],
                          self.job_id,
                          self.par_dic,
                          self.token)

        self.logger.info("%s.run_call_back with args %s", self, self.par_dic)
        self.logger.info("%s.run_call_back built job %s", self, job)

        if job.status_kw_name in self.par_dic.keys():
            status = self.par_dic[job.status_kw_name]
        else:
            status = 'unknown'
            
        self.logger.info(f'-----> set status to {status}')

        if self.is_email_to_send_callback(status):
            try:
                # build the products URL
                request_url = self.generate_request_url_call_back(self.config.products_url, session_id, self.job_id)
                self.send_email(status,
                                instrument=instrument_name,
                                time_request=self.time_request,
                                request_url=request_url)
                job.write_dataserver_status(status_dictionary_value=status,
                                            full_dict=self.par_dic,
                                            email_status='email sent')
            except EMailNotSent as e:
                job.write_dataserver_status(status_dictionary_value=status,
                                            full_dict=self.par_dic,
                                            email_status='sending email failed')
                logging.warning(f'email sending failed: {e}')
                if self.sentry_client is not None:
                    self.sentry_client.capture('raven.events.Message',
                                               message=f'sending email failed {e}')
        else:
            job.write_dataserver_status(status_dictionary_value=status, full_dict=self.par_dic)


    def generate_request_url_call_back(self, products_url, session_id, job_id) -> str:
        job_monitor_status_json_file = f'scratch_sid_{session_id}_jid_{job_id}/query_output.json'
        # to be handled now, with the job_id generated taking into account only the user_id
        job_monitor_status_json_file_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/query_output.json'
        request_url = ""
        file = None
        if self.scratch_dir:
            file = open(self.scratch_dir + '/query_output.json')
        else:
            if os.path.exists(job_monitor_status_json_file_aliased):
                file = open(job_monitor_status_json_file_aliased)
            elif os.path.exists(job_monitor_status_json_file):
                file = open(job_monitor_status_json_file)
        if file:
            jdata = json.load(file)
            if 'prod_dictionary' in jdata and 'analysis_parameters' in jdata['prod_dictionary']:
                request_par_dict = jdata['prod_dictionary']['analysis_parameters']
                request_url = '%s?%s' % (products_url, urlencode(request_par_dict))
        return request_url

    def is_email_to_send_run_completion(self, status):
        # get total request duration
        if not self.public:
            email_sending_job_submitted = tokenHelper.get_token_user_submitted_email(self.decoded_token)
            if email_sending_job_submitted is None:
                # in case this didn't come with the token take the default value
                email_sending_job_submitted = self.app.config.get('conf').email_sending_job_submitted
            # send submitted mail, status update
            return email_sending_job_submitted and status == 'submitted'

        return False

    def is_email_to_send_callback(self, status):
        # get total request duration
        duration_query = -1
        if self.time_request:
            duration_query = time_.time() - self.time_request
        if not self.public:
            timeout_threshold_email = tokenHelper.get_token_user_timeout_threshold_email(self.decoded_token)
            if timeout_threshold_email is None:
                # set it to the a default value, from the configuration
                timeout_threshold_email = self.app.config.get('conf').email_sending_timeout_default_threshold
            email_sending_timeout = tokenHelper.get_token_user_sending_timeout_email(self.decoded_token)
            if email_sending_timeout is None:
                email_sending_timeout = self.app.config.get('conf').email_sending_timeout
            # in case the request was long and 'done'
            # or if failed
            # or when the job was created ('submitted')
            return (email_sending_timeout and duration_query > timeout_threshold_email and status == 'done') or status == 'failed'
                   # or status == 'submitted'

        return False

    def run_query_mock(self, off_line=False):

        #job_status = self.par_dic['job_status']
        session_id = self.par_dic['session_id']

        if 'instrumet' in self.par_dic.keys():
            self.par_dic.pop('instrumet')

        self.logger.info('instrument %s' % self.instrument_name)
        self.logger.info('parameters dictionary')

        for key in self.par_dic.keys():
            log_str = 'parameters dictionary, key=' + \
                key + ' value=' + str(self.par_dic[key])
            self.logger.info(log_str)

        out_dict = mock_query(self.par_dic, session_id,
                              self.job_id, self.scratch_dir)

        self.logger.info('============================================================')
        self.logger.info('')

        if off_line == False:
            response = jsonify(out_dict)
        else:
            response = out_dict

        return response

    def build_dispatcher_response(self,
                                  query_new_status=None,
                                  query_out=None,
                                  status_code=None,
                                  job_monitor=None,
                                  off_line=True,
                                  api=False,):

        out_dict = {}

        if query_new_status is not None:
            out_dict['query_status'] = query_new_status
        if query_out is not None:
            out_dict['products'] = query_out.prod_dictionary
            out_dict['exit_status'] = query_out.status_dictionary

        if job_monitor is not None:
            out_dict['job_monitor'] = job_monitor
            out_dict['job_status'] = job_monitor['status']

        if job_monitor is not None:
            out_dict['job_monitor'] = job_monitor

        out_dict['session_id'] = self.par_dic['session_id']

        if status_code is not None:
            out_dict['status_code'] = status_code

        if off_line:
            return out_dict
        else:
            try:
                if api:
                    return self.jsonify_api_response(out_dict), status_code
                else:
                    return jsonify(out_dict), status_code

            except Exception as e:
                self.logger.error(f'Failed when building the output response: {e}')
                if query_out is None:
                    query_out = QueryOutput()
                else:
                    pass

                query_out.set_failed('build dispatcher response',
                                     extra_message='failed json serialization',
                                     debug_message=str(getattr(e, 'message', repr(e))))

                out_dict['exit_status'] = query_out.status_dictionary

                return jsonify(out_dict), status_code

    def jsonify_api_response(self, out_dict):
        return jsonify(self.prep_jsonify_api_response(out_dict))

    def prep_jsonify_api_response(self, out_dict):
        if 'numpy_data_product_list' in out_dict['products']:
            _npdl = out_dict['products']['numpy_data_product_list']

            out_dict['products']['numpy_data_product_list'] = [
                (_d.encode() if isinstance(_d, NumpyDataProduct) else _d)  # meh TODO
                for _d in _npdl
            ]

        return out_dict

    def set_instrument(self, instrument_name, data_server_call_back=False):

        if instrument_name is None:
            if 'instrument' in self.par_dic:
                self.instrument_name = self.par_dic['instrument']
            else:
                raise NoInstrumentSpecified(
                    f"have paramters: {list(self.par_dic.keys())}")
        else:
            self.instrument_name = instrument_name

        if not data_server_call_back:
            known_instruments = []

            new_instrument = None
            for instrument_factory in importer.instrument_factory_list:
                instrument = instrument_factory()
                # name must be unique!
                if instrument.name == self.instrument_name:
                    new_instrument = instrument
                known_instruments.append(instrument.name)

            if new_instrument is None:
                raise InstrumentNotRecognized(
                    f'instrument: "{instrument_name}", known: {known_instruments}')
            else:
                self.instrument = new_instrument

    def set_config(self):
        if getattr(self, 'config', None) is None:
            config = self.app.config.get('conf')
        else:
            config = self.config

        disp_data_server_conf_dict = config.get_data_server_conf_dict(self.instrument_name)

        # instrument may be not set in callback call

        instrument = getattr(self, 'instrument', None)
        
        if disp_data_server_conf_dict is None:
            if instrument is not None and not isinstance(instrument, str):
                logger.debug('provided instrument type %s', type(instrument))
                disp_data_server_conf_dict = self.instrument.data_server_conf_dict
            
        logger.debug('--> App configuration for: %s', self.instrument_name)
        if disp_data_server_conf_dict is not None:
            if 'data_server' in disp_data_server_conf_dict.keys():
                if self.instrument.name in disp_data_server_conf_dict['data_server'].keys():
                    for k in disp_data_server_conf_dict['data_server'][self.instrument.name].keys():
                        if k in self.instrument.data_server_conf_dict.keys():
                            self.instrument.data_server_conf_dict[k] = disp_data_server_conf_dict[
                                'data_server'][self.instrument.name][k]

            self.config_data_server = DataServerConf.from_conf_dict(
                self.instrument.data_server_conf_dict)
        else:
            self.config_data_server = None
        # if hasattr(self,'instrument'):
            # config_data_server=DataServerConf.from_conf_dict(self.instrument.data_server_conf_dict)

        logger.info("loaded config %s", config)

        return config, self.config_data_server

    def get_existing_job_ID_path(self, wd):
        # exist same job_ID, different session ID
        # if self.job_id is None:
        #     dir_list = []
        # else:
        dir_list = glob.glob('*_jid_%s' % (self.job_id))
        if dir_list != []:
            dir_list = [d for d in dir_list if 'aliased' not in d]

        if len(dir_list) == 1:
            if dir_list[0] != wd:
                alias_dir = dir_list[0]
            else:
                alias_dir = None

        elif len(dir_list) > 1:
            raise RuntimeError('found two non aliased identical job_id')

        else:
            alias_dir = None

        return alias_dir

    def get_file_mtime(self, file):
        return os.path.getmtime(file)

    def find_api_version_issues(self, off_line, api) -> typing.Union[bool, QueryOutput]:
        current_disp_oda_api_version = None
        if hasattr(oda_api, '__version__'):
            current_disp_oda_api_version = oda_api.__version__

        query_oda_api_version = None

        if 'oda_api_version' in self.par_dic.keys():
            query_oda_api_version = self.par_dic['oda_api_version']

        oda_api_version_error = None
        failed_task = 'oda_api version compatibility'

        if query_oda_api_version is None:
            oda_api_version_error = 'oda_api version compatibility non safe, please update your oda_api package'
        elif current_disp_oda_api_version is None:
            oda_api_version_error = 'oda_api on server are outdated please contact oda api responsible'
        elif current_disp_oda_api_version > query_oda_api_version:
            oda_api_version_error = f'oda_api version not compatible, ' + \
                                    f'min version={current_disp_oda_api_version}, oda api query version={query_oda_api_version}, ' + \
                                    f'please update your oda_api package'
        else:
            pass

        self.logger.warning(
            "find_api_version_issues issue: %s, %s", oda_api_version_error, failed_task)
        # TODO: and return something maybe

        return None  # it's good

    def validate_query_from_token(self):
        """
        read base64 token
        decide if it is valid
        return True/False
        """

        """
        extract the various content of the token
        """
        # decode the token
        # self.decoded_token = self.get_decoded_token()
        secret_key = self.app.config.get('conf').secret_key
        self.decoded_token = tokenHelper.get_decoded_token(self.token, secret_key)
        self.logger.info("==> token %s", self.decoded_token)
        return True

    def build_job(self):
        return job_factory(self.instrument_name,
                           self.scratch_dir,
                           self.dispatcher_host,
                           self.dispatcher_port,
                           self.dispatcher_callback_url_base,
                           self.par_dic['session_id'],
                           self.job_id,
                           self.par_dic,
                           aliased=False)

    def build_response_failed(self, message, extra_message, status_code=None):
        job = self.build_job()
        job.set_failed()
        job_monitor = job.monitor

        query_status = 'failed'

        query_out = QueryOutput()

        failed_task = message

        query_out.set_failed(failed_task,
                             message=extra_message,
                             job_status=job_monitor['status'])

        resp = self.build_dispatcher_response(query_new_status=query_status,
                                              query_out=query_out,
                                              job_monitor=job_monitor,
                                              status_code=status_code,
                                              off_line=self.off_line,
                                              api=self.api)
        return resp

    def validate_token_request_param(self):
        # if the request is public then authorize it, because the token is not there
        if self.public:
            return None

        if self.token is not None:
            try:
                if self.validate_query_from_token():
                    return None
                else:
                    # in case the token is not valid returns an empty object
                    return {}
            except jwt.exceptions.ExpiredSignatureError as e:
                # expired token
                return self.build_response_failed('oda_api permissions failed', 'token expired')
            except Exception as e:
                return self.build_response_failed('oda_api permissions failed',
                                                  'you do not have permissions for this query, contact oda')
        else:
            self.logger.warning('==> NO TOKEN FOUND IN PARS')
            return self.build_response_failed('oda_api token is needed',
                                              'you do not have permissions for this query, contact oda')

    @property
    def query_log_dir(self):
        return os.path.dirname(self.response_log_filename)

    @property
    def response_filename(self):
        return os.path.join(self.scratch_dir, "query_output.json")

    @property
    def response_log_filename(self):
        return os.path.join(self.scratch_dir,
                            "query-log",
                            f"query_output_{time_.strftime('%Y-%m-%d-%H-%M-%S')}.json")

    @property
    def response_request(self):
        # this file-based stuff is vulnerable to race conditions, and can become problematic
        # luckily dispatcher is usually scales to few processes at most
        return os.path.join(self.scratch_dir, "query_output_request.json")

    def find_stored_response(self) -> QueryOutput:
        if os.path.exists(self.response_filename):
            self.logger.info(
                "\033[32mstored query out FOUND at %s\033[0m", self.response_filename)
            Q = QueryOutput()

            try:
                Q.deserialize(open(self.response_filename, "r"))
                j = json.load(open(self.response_filename +
                                   ".job-monitor", "r"))  # modify!
            except (ProblemDecodingStoredQueryOut, FileNotFoundError, json.decoder.JSONDecodeError) as e:
                self.logger.info(
                    "\033[31mstored query out corrupt (race?) or NOT FOUND at %s\033[0m", self.response_filename)
                return

            return Q, j

        self.logger.info(
            "\033[31mstored query out NOT FOUND at %s\033[0m", self.response_filename)

    def request_query_out(self, overwrite=False):
        if os.path.exists(self.response_request):
            r_json = json.load(open(self.response_request))

            r = tasks.celery.AsyncResult(r_json['celery-id'])
            r_state = r.state

            self.logger.info("found celery job: %s state: %s", r.id, r_state)
            self.logger.info("celery job: %s state: %s", r, r.__dict__)

            if r_state == "PENDING":
                flower_task = tasks.flower_task(r_json['celery-id'])
                if flower_task is None:
                    self.logger.info("PENDING celery job: %s does not exist in flower, marking UNEXISTENT", r)
                    r_state = "UNEXISTENT"
            
            if r_state in ["FAILURE"]:
                self.logger.info("celery job state failure, will overwrite")
                overwrite = True

            if not overwrite:
                self.logger.info("not overwriting, fine with the job")
                return
            else:
                if r_state in ["PENDING", "RUNNING"]:
                    self.logger.info(
                        "even with overwriting, will not touch running/pending active job: %s", r_state) # sometimes job is stuck??
                    return
                else:
                    self.logger.info(
                        "overwriting request for this job: %s", r_state)

        # TODO: here we might as well query from minio etc, but only if ready
        r = tasks.request_dispatcher.apply_async(
            args=[self.dispatcher_callback_url_base + "/run_analysis"],
            kwargs={**self.par_dic, 'async_dispatcher': False}
        )
        self.logger.info("submitted celery job with pars %s", self.par_dic)
        self.logger.info("submitted celery job: %s state: %s", r.id, r.state)
        json.dump({'celery-id': r.id},
                  open(self.response_request, "w"))

    def store_response(self, query_out, job_monitor):
        self.logger.info("storing query output: %s, %s",
                         self.response_filename, self.response_log_filename)
        if os.path.exists(self.response_filename):
            if not os.path.exists(self.query_log_dir):
                os.makedirs(self.query_log_dir)
            os.rename(self.response_filename, self.response_log_filename)
            self.logger.info("renamed query log log %s => %s",
                             self.response_filename, self.response_log_filename)

        query_out.serialize(open(self.response_filename, "w"))
        json.dump(job_monitor, open(
            self.response_filename + ".job-monitor", "w"))

    def load_config(self):
        try:
            config, self.config_data_server = self.set_config()
            self.logger.info(
                'loading config: %s config_data_server: %s', config, self.config_data_server)
            self.logger.info('dispatcher port %s', config.bind_port)
        except Exception as e:
            self.logger.error("problem setting config %s", e)

            # ?better not
            query_out = QueryOutput()
            query_out.set_query_exception(e, 'run_query failed in %s' % self.__class__.__name__,
                                          extra_message='configuration failed')

            config, self.config_data_server = None, None
        else:
            if config.sentry_url is not None:
                self.set_sentry_client(config.sentry_url)

            self.config = config


    def run_query(self, off_line=False, disp_conf=None):
        """
        this is the principal function to respond to the requests

        TODO: this function is a bit quite very long, and flow is a little bit too complex, especially for exception handling
        """

        self.off_line = off_line

        self.logger.info(
            '\033[31;42m==============================> run query <==============================\033[0m')
        if 'api' in self.par_dic.keys():
            api = True            

            r = self.find_api_version_issues(off_line, api) # pylint: disable=assignment-from-none

            if r is not None:
                if os.environ.get('DISPATCHER_ENFORCE_API_VERSION', 'no') == 'yes':
                    self.logger.warning(
                        "client API has incompatible version: %s, and it is not ok!", r)
                    return r
                else:
                    self.logger.warning(
                        "client API has incompatible version: %s, but it is ok", r)
        else:
            api = False

        self.api = api # TODO: we should decide if it's memeber or not

        try:
            query_type = self.par_dic['query_type']
            product_type = self.par_dic['product_type']
            query_status = self.par_dic['query_status']
        except KeyError as e:
            raise MissingRequestParameter(repr(e))

        # resp = self.validate_token_request_param()
        # if resp is not None:
        #     self.logger.warning("query dismissed by token validation")
        #     return resp

        if self.par_dic.pop('instrumet', None) is not None:
            self.logger.warning("someone is sending instrume(N!)ts?")

        verbose = self.par_dic.get('verbose', 'False') == 'True'  # ??
        dry_run = self.par_dic.get('dry_run', 'False') == 'True'  # ??

        self.logger.info('product_type %s', product_type)
        self.logger.info('query_type %s ', query_type)
        self.logger.info('instrument %s', self.instrument_name)
        self.logger.info('parameters dictionary')

        for k, v in self.par_dic.items():
            self.logger.debug('parameters dictionary, key=%s value=%s', k, v)

        self.load_config()

        alias_workdir = None
        try:
            alias_workdir = self.get_existing_job_ID_path(self.scratch_dir)
        except Exception as e:
            query_out = QueryOutput()
            query_out.set_query_exception(e,
                                          'run_query failed in %s' % self.__class__.__name__,
                                          extra_message='job aliasing failed')

        job_is_aliased = False
        run_asynch = True

        if 'run_asynch' in self.par_dic.keys():
            if self.par_dic['run_asynch'] == 'True':
                run_asynch = True
            elif self.par_dic['run_asynch'] == 'False':
                run_asynch = False
            else:
                raise RuntimeError(
                    'run_asynch can be True or False, found', self.par_dic['run_asynch'])

        if self.async_dispatcher:
            self.logger.info('==> async dispatcher operation requested')
        else:
            self.logger.info('==> async dispatcher operation NOT requested')

        if self.instrument.asynch == False:
            run_asynch = False

        if alias_workdir is not None and run_asynch:
            job_is_aliased = True

        self.logger.info('--> is job aliased? : %s', job_is_aliased)
        job = job_factory(self.instrument_name,
                          self.scratch_dir,
                          self.dispatcher_host,
                          self.dispatcher_port,
                          self.dispatcher_callback_url_base,
                          self.par_dic['session_id'],
                          self.job_id,
                          self.par_dic,
                          aliased=job_is_aliased,
                          token=self.token)

        job_monitor = job.monitor

        self.logger.info(
            '-----------------> query status  old is: %s', query_status)
        self.logger.info(
            '-----------------> job status before query: %s', job.status)
        self.logger.info(
            '-----------------> job_is_aliased: %s', job_is_aliased)

        out_dict = None
        query_out = None

        # TODO if query status== ready but you get delegation
        # TODO set query status to new and ignore alias

        if job_is_aliased and query_status != 'ready':
            job_is_aliased = True

            original_work_dir = job.work_dir
            job.work_dir = alias_workdir

            self.logger.info(
                '\033[32m==> ALIASING to %s\033[0m', alias_workdir)

            try:
                job_monitor = job.updated_dataserver_monitor()
            except:
                job_is_aliased = False
                job_monitor = {}
                job_monitor['status'] = 'failed'

            self.logger.info('==>updated job_monitor %s', job_monitor['status'])

            if job_monitor['status'] == 'ready' or job_monitor['status'] == 'failed' or job_monitor['status'] == 'done':
                # NOTE in this case if job is aliased but the original has failed
                # NOTE it will be resubmitted anyhow
                self.logger.info('==>aliased job status %s', job_monitor['status'])
                job_is_aliased = False
                job.work_dir = original_work_dir
                job_monitor = job.updated_dataserver_monitor()
                # Note this is necessary to avoid a never ending loop in the non-aliased job-status is set to progress
                self.logger.info('query_status', query_status)

                query_status = 'new'
                self.logger.info('==>ALIASING switched off  for status %s', job_monitor['status'])

                if query_type == 'Dummy':
                    job_is_aliased = False
                    job.work_dir = original_work_dir
                    job_monitor = job.updated_dataserver_monitor()
                    self.logger.info('==>ALIASING switched off for Dummy query')

        if job_is_aliased and query_status == 'ready':
            original_work_dir = job.work_dir
            job.work_dir = alias_workdir

            job_is_aliased = False
            job.work_dir = original_work_dir
            job_monitor = job.updated_dataserver_monitor()
            self.logger.info('==>ALIASING switched off for status ready')

        if job_is_aliased:
            delta_limit = 600
            try:
                delta = self.get_file_mtime(
                    alias_workdir + '/' + 'job_monitor.json') - time.time()
            except:
                delta = delta_limit+1

            if delta > delta_limit:
                original_work_dir = job.work_dir
                job.work_dir = alias_workdir

                job_is_aliased = False
                job.work_dir = original_work_dir
                job_monitor = job.updated_dataserver_monitor()
                self.logger.info('==>ALIASING switched off for delta time >%f, delta=%f' %
                      (delta_limit, delta))

        self.logger.info('==> aliased is %s', job_is_aliased)
        self.logger.info('==> alias  work dir %s', alias_workdir)
        self.logger.info('==> job  work dir %s', job.work_dir)
        self.logger.info('==> query_status  %s', query_status)

        if (query_status == 'new' and job_is_aliased == False) or query_status == 'ready':
            self.logger.info('*** run_asynch %s', run_asynch)
            self.logger.info('*** api %s', api)
            self.logger.info('config_data_server %s', self.config_data_server)

            self.instrument.disp_conf = disp_conf

            # this might be long and we want to async this

            if self.async_dispatcher:
                query_out, job_monitor, query_new_status = self.async_dispatcher_query(
                    query_status)
                if job_monitor is None:
                    job_monitor = job.monitor
            else:
                try:
                    query_out = self.instrument.run_query(product_type,
                                                          self.par_dic,
                                                          request,
                                                          self,  # this will change?
                                                          job,  # this will change
                                                          run_asynch,
                                                          out_dir=self.scratch_dir,
                                                          config=self.config_data_server,
                                                          query_type=query_type,
                                                          # logger=self.logger,
                                                          sentry_client=self.sentry_client,
                                                          verbose=verbose,
                                                          dry_run=dry_run,
                                                          api=api,
                                                          decoded_token=self.decoded_token)
                except RequestNotAuthorized as e:
                    return self.build_response_failed('oda_api permissions failed',
                                                      e.message,
                                                      status_code=e.status_code)

                self.logger.info('-----------------> job status after query: %s', job.status)

                if query_out.status_dictionary['status'] == 0:
                    if job.status == 'done':
                        query_new_status = 'done'
                    elif job.status == 'failed':
                        query_new_status = 'failed'
                    else:
                        query_new_status = 'submitted'
                        job.set_submitted()
                    # mail sending ?
                    if self.is_email_to_send_run_completion(query_new_status):
                        try:
                            request_url = '%s?%s' % (self.app.config.get('conf').products_url, urlencode(self.par_dic))
                            self.send_email('submitted',
                                            instrument=self.instrument.name,
                                            time_request=self.time_request,
                                            request_url=request_url)
                            # store an additional information about the sent email
                            query_out.set_status_field('email_status', 'email sent')
                        except EMailNotSent as e:
                            query_out.set_status_field('email_status', 'sending email failed')
                            logging.warning(f'email sending failed: {e}')
                            if self.sentry_client is not None:
                                self.sentry_client.capture('raven.events.Message',
                                                           message=f'sending email failed: {e.message}')
                else:
                    query_new_status = 'failed'
                    job.set_failed()

                job.write_dataserver_status()

            self.logger.info('-----------------> query status update for done/ready: %s', query_new_status)

        elif query_status == 'progress' or query_status == 'unaccessible' or query_status == 'unknown' or query_status == 'submitted':
            # we can not just avoid async here since the request still might be long
            if self.async_dispatcher:
                query_out, job_monitor, query_new_status = self.async_dispatcher_query(
                    query_status)

                if job_monitor is None:
                    job_monitor = job.monitor
            else:
                query_out = QueryOutput()

                job_monitor = job.updated_dataserver_monitor()

                self.logger.info(
                    '-----------------> job monitor from data server: %s', job_monitor['status'])

                if job_monitor['status'] == 'done':
                    job.set_ready()

                query_out.set_done(job_status=job_monitor['status'])

                if job_monitor['status'] in ['unaccessible', 'unknown']:
                    query_new_status = query_status
                else:
                    query_new_status = job.get_status()

            self.logger.info('-----------------> job monitor updated %s',
                  job_monitor['status'])
            self.logger.info('-----------------> query status update for progress: %s',
                  query_new_status)

        elif query_status == 'failed':
            # TODO: here we should resubmit query to get exception from ddosa
            query_out = QueryOutput()
            query_out.set_failed(
                'submitted job', job_status=job_monitor['status'])

            query_new_status = 'failed'
            self.logger.info('-----------------> query status update for failed: %s',
                  query_new_status)
            self.logger.info(
                '==============================> query done <==============================')

        else:
            query_out = QueryOutput()
            query_out.set_status(0, job_status=job_monitor['status'])

            query_new_status = job.get_status()

            self.logger.info(
                'query_out:job_monitor[status]: %s', job_monitor['status'])
            self.logger.info(
                '-----------------> query status now: %s', query_new_status)
            self.logger.info(
                '==============================> query done <==============================')

        if not job_is_aliased:
            job.write_dataserver_status()

        if not self.async_dispatcher:
            # should we store entire reponse, before it is serialized?..
            self.store_response(query_out, job_monitor)

        self.logger.info(
            '\033[33;44m============================================================\033[0m')
        self.logger.info('')

        resp = self.build_dispatcher_response(query_new_status=query_new_status,
                                              query_out=query_out,
                                              job_monitor=job_monitor,
                                              off_line=off_line,
                                              api=api)
        return resp

    def send_email(self, status="done",
                   instrument="",
                   time_request="",
                   request_url=""):
        server = None
        self.logger.info("Sending email")
        time_request_str = ""
        if time_request != "":
            time_request_str = time_.strftime('%Y-%m-%d %H:%M:%S', time_.localtime(float(time_request)))
        try:
            # send the mail with the status update to the mail provided with the token
            # eg done/failed/submitted
            # test with the local server
            smtp_server = self.app.config.get('conf').smtp_server
            port = self.app.config.get('conf').smtp_port
            sender_email_address = self.app.config.get('conf').sender_email_address
            cc_receivers_email_addresses = self.app.config.get('conf').cc_receivers_email_addresses
            receiver_email_address = tokenHelper.get_token_user_email_address(self.decoded_token)
            receivers_email_addresses = [receiver_email_address] + cc_receivers_email_addresses
            # creation of the message
            message = MIMEMultipart("alternative")
            message["Subject"] = "Request update"
            message["From"] = sender_email_address
            message["To"] = receiver_email_address
            message["CC"] = ", ".join(cc_receivers_email_addresses)

            # Create the plain-text and HTML version of your message,
            # since enails with HTML content might be, sometimes, not supportenot
            # a plain-text version is included
            text = f"""Update of the task submitted at {time_request_str}, for the instrument {instrument}:\n* status {status}\nProducts url {request_url}"""
            html = f"""<html><body><p>Update of the task submitted at {time_request_str}, for the instrument {instrument}:<br><ul><li>status {status}</li></ul>Products url {request_url}</p></body></html>"""

            part1 = MIMEText(text, "plain")
            part2 = MIMEText(html, "html")
            message.attach(part1)
            message.attach(part2)

            smtp_server_password = self.app.config.get('conf').smtp_server_password
            # Create a secure SSL context
            context = ssl.create_default_context()
            #
            # Try to log in to server and send email
            server = smtplib.SMTP(smtp_server, port)
            # just for testing purposes, not ssl is established
            if smtp_server != "localhost":
                server.starttls(context=context)
            if smtp_server_password is not None and smtp_server_password != '':
                server.login(sender_email_address, smtp_server_password)
            server.sendmail(sender_email_address, receivers_email_addresses, message.as_string())
        except Exception as e:
            self.logger.error(f'Exception while sending email: {e}')
            raise EMailNotSent(f"email not sent {e}")
        finally:
            if server:
                server.quit()

    def async_dispatcher_query(self, query_status: str) -> tuple:
        self.logger.info("async dispatcher enabled, for %s", query_status)

        R = self.find_stored_response()

        if R is None:
            query_new_status = 'submitted'

            self.logger.info(
                "async dispatcher query_out not ready, registering")
            self.request_query_out()

            job_monitor = None

            query_out = QueryOutput()
            # is this acceptable to frontend?
            query_out.set_status(
                status=0, job_status="post-processing", message="async-dispatcher waiting")

        else:
            query_out, job_monitor = R

            self.logger.info("\033[32masync dispatcher query_out READY, new status %s job_status %s\033[0m",
                             query_out.status_dictionary['status'],
                             query_out.status_dictionary['job_status'],
                             )

            if query_out.status_dictionary['status'] == 0:
                job_status = query_out.status_dictionary['job_status']
            else:
                job_status = "failed"
                self.logger.warning(
                    "why is status not 0? it is %s", query_out.status_dictionary['status'])

            # if job_status in ['done', 'ready']: #two??
            if job_status in ['done']:
                query_new_status = 'done'

            elif job_status == 'failed':
                query_new_status = 'failed'

            else:
                if job_status in ["progress", "ready"]:
                    query_new_status = job_status
                else:
                    query_new_status = 'submitted'

                self.request_query_out(overwrite=True)
                self.logger.info("\033[36mforce RESUBMIT for this job_status=%s, will query_new_status=%s!\033[0m",
                                 job_status,
                                 query_new_status)

                query_out = QueryOutput()
                # is this acceptable to frontend?
                query_out.set_status(
                    status=0, job_status="submitted", message="async-dispatcher waiting")

        return query_out, job_monitor, query_new_status
