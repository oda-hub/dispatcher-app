#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May 10 10:55:20 2017

@author: andrea tramcere
"""
import os
import pathlib
import time
from builtins import (open, str, range,
                      object)

import traceback

from collections import Counter, OrderedDict
import copy

# import logging
# from werkzeug.utils import secure_filename

import glob
import string
import random
import fcntl

from flask import jsonify, send_from_directory, make_response
from flask import request, g
import time as time_

import tempfile
import tarfile
import gzip
import socket
import logstash
import shutil
import jwt
import re
import logging
import json
import typing

from ..plugins import importer
from ..analysis.queries import SourceQuery
from ..analysis import tokenHelper, email_helper, matrix_helper
from ..analysis.instrument import params_not_to_be_included
from ..analysis.hash import make_hash
from ..analysis.hash import default_kw_black_list
from ..analysis.job_manager import job_factory
from ..analysis.io_helper import FilePath, format_size
from .mock_data_server import mock_query
from ..analysis.products import QueryOutput
from ..configurer import DataServerConf
from ..analysis.exceptions import BadRequest, APIerror, MissingRequestParameter, RequestNotUnderstood, RequestNotAuthorized, ProblemDecodingStoredQueryOut, InternalError
from . import tasks
from ..flask_app.sentry import sentry

from oda_api.api import DispatcherAPI

from .logstash import logstash_message

from oda_api.data_products import NumpyDataProduct
import oda_api
from cdci_data_analysis.timer import block_timer

logger = logging.getLogger(__name__)


class NoInstrumentSpecified(BadRequest):
    pass


class InstrumentNotRecognized(BadRequest):
    pass


class InvalidJobIDProvided(BadRequest):
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

    @property
    def query_progression(self):
        if not hasattr(self, '_query_progression'):
            self._query_progression = []
        
        return self._query_progression

    def log_query_progression(self, message):        
        self.query_progression.append(dict(
                t_s = time_.time(),
                message = message,                
            ))

        t0 = self.query_progression[0]['t_s']
        self.logger.warning("%s %s s", message, self.query_progression[-1]['t_s'] - t0)

    def __init__(self, app,
                 instrument_name=None,
                 par_dic=None,
                 config=None,
                 data_server_call_back=False,
                 verbose=False,
                 get_meta_data=False,
                 download_products=False,
                 download_files=False,
                 resolve_job_url=False,
                 query_id=None,
                 update_token=False):
        self.logger = logging.getLogger(f"{repr(self)} [{query_id}]")
        self.logger = logging.getLogger(repr(self))

        if verbose:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        self.request_files_dir = self.get_request_files_dir()

        params_not_to_be_included.clear()
        params_not_to_be_included.append('user_catalog')

        self.app = app

        temp_scratch_dir = None
        temp_job_id = None

        self.set_sentry_sdk(getattr(self.app.config.get('conf'), 'sentry_url', None))

        try:
            if par_dic is None:
                self.set_args(request, verbose=verbose, download_files=download_files, download_products=download_products)
            else:
                self.par_dic = par_dic
            self.log_query_progression("after set args")

            self.log_query_progression("before set_session_id")
            self.set_session_id()
            self.log_query_progression("after set_session_id")

            if data_server_call_back or resolve_job_url:
                # in the case of call_back or resolve_job_url the job_id can be extracted from the received par_dic
                self.job_id = None
                if 'job_id' in self.par_dic:
                    self.job_id = self.par_dic['job_id']
                else:
                    sentry.capture_message("job_id not present during a call_back")
                    raise RequestNotUnderstood("job_id must be present during a call_back")
            if data_server_call_back:
                # this can be set since it's a call_back and job_id and session_id are available
                self.logger.info(f"before setting scratch_dir: job_id: {self.par_dic['job_id']} callback: {data_server_call_back}, resolve_job_url: {resolve_job_url}")
                self.set_scratch_dir(session_id=self.par_dic['session_id'], job_id=self.par_dic['job_id'])
                self.set_session_logger(self.scratch_dir, verbose=verbose, config=config)
                self.logger.info(f"scratch_dir set {self.scratch_dir}, job_id: {self.par_dic['job_id']} callback: {data_server_call_back}, resolve_job_url: {resolve_job_url}")
                self.set_scws_call_back_related_params()
                self.logger.info(f"set_scws_call_back_related_params executed")
            else:
                self.set_scws_related_params(request)


            self.client_name = self.par_dic.pop('client-name', 'unknown')
            self.return_progress = self.par_dic.pop('return_progress', False) == 'True'
            if os.environ.get("DISPATCHER_ASYNC_ENABLED", "no") == "yes":  # TODO: move to config!
                self.async_dispatcher = self.par_dic.pop(
                    'async_dispatcher', 'True') == 'True'  # why string true?? else false anyway
            else:
                self.async_dispatcher = False
            """
                async dispatcher operation avoids building QueryOutput in the sync request, and instead sends it in the queue
                in the queue, the same request is repeated, same session id/job id, but requesting sync request
                this immitates two repeated identical requests from the same client, which takes care of aliasing/etc complexity
                the remaining complexity is to send back a response which indicates "submitted" but not submitted job - only request
            """

            self.time_request = g.get('request_start_time', None)

            # By default, a request is public, let's now check if a token has been included
            # In that case, validation is needed
            self.public = True
            self.token = None
            email=None
            roles=None
            self.decoded_token = None
            if 'token' in self.par_dic.keys() and self.par_dic['token'] not in ["", "None", None]:
                self.token = self.par_dic['token']
                self.public = False
                # token validation and decoding can be done here, to check if the token is expired
                self.log_query_progression("before validate_query_from_token")
                try:
                    if self.validate_query_from_token():
                        roles = tokenHelper.get_token_roles(self.decoded_token)
                        email = tokenHelper.get_token_user_email_address(self.decoded_token)

                except jwt.exceptions.ExpiredSignatureError as e:
                    logstash_message(app, {'origin': 'dispatcher-run-analysis', 'event': 'token-expired'})
                    message = ("The token provided is expired, please try to logout and login again. "
                               "If already logged out, please clean the cookies, "
                               "and resubmit you request.")
                    if data_server_call_back:
                        message = "The token provided is expired, please resubmit you request with a valid token."
                        self.logger.info(message)
                        sentry.capture_message(message)

                    raise RequestNotAuthorized(message)
                except jwt.exceptions.InvalidSignatureError as e:
                    logstash_message(app, {'origin': 'dispatcher-run-analysis', 'event': 'not-valid-token'})
                    message = ("The token provided is not valid, please try to logout and login again. "
                               "If already logged out, please clean the cookies, "
                               "and resubmit you request.")
                    if data_server_call_back:
                        message = "The token provided is expired, please resubmit you request with a valid token."
                        self.logger.info(message)
                        sentry.capture_message(message)

                    raise RequestNotAuthorized(message)

                self.log_query_progression("after validate_query_from_token")
                logstash_message(app, {'origin': 'dispatcher-run-analysis', 'event':'token-accepted', 'decoded-token':self.decoded_token })
                self.log_query_progression("after logstash_message")

            if download_products or resolve_job_url or update_token or download_files:
                instrument_name = 'mock'

            self.logger.info("before setting instrument, self.par_dic: %s", self.par_dic)

            if instrument_name is None:
                if 'instrument' in self.par_dic:
                    self.instrument_name = self.par_dic['instrument']
                else:
                    self.logger.error("NoInstrumentSpecified, self.par_dic: %s", self.par_dic)
                    raise NoInstrumentSpecified(
                        f"have parameters: {list(self.par_dic.keys())} ")
            else:
                self.instrument_name = instrument_name

            if get_meta_data:
                self.logger.info("get_meta_data request: no scratch_dir")
                self.set_instrument(self.instrument_name, roles, email)
                # TODO
                # decide if it is worth to add the logger also in this case
                #self.set_scratch_dir(self.par_dic['session_id'], verbose=verbose)
                #self.set_session_logger(self.scratch_dir, verbose=verbose, config=config)
                # self.set_sentry_client()
            else:
                logger.debug("NOT get_meta_data request: yes scratch_dir")
                # TODO why here and not at the beginning ?
                # self.set_sentry_client()
                # TODO is also the case of call_back to handle ?
                if not data_server_call_back:
                    self.set_instrument(self.instrument_name, roles, email)

                # TODO: if not callback!
                # if 'query_status' not in self.par_dic:
                #    raise MissingRequestParameter('no query_status!')

                verbose = self.par_dic.get('verbose', 'False') == 'True'
                if not (data_server_call_back or resolve_job_url or download_files):
                    query_status = self.par_dic['query_status']
                    self.job_id = None
                    if query_status == 'new':
                        # let's generate a temporary job_id used for the creation of the scratch_dir
                        self.generate_job_id()
                    else:
                        if 'job_id' not in self.par_dic:
                            raise RequestNotUnderstood(
                                f"job_id must be present if query_status != \"new\" (it is \"{query_status}\")")

                        self.job_id = self.par_dic['job_id']

                if not download_files:
                    # let's generate a temporary scratch_dir using the temporary job_id
                    self.set_scratch_dir(self.par_dic['session_id'], job_id=self.job_id, verbose=verbose)
                    # temp_job_id = self.job_id
                    temp_scratch_dir = self.scratch_dir
                    temp_job_id = self.job_id
                if not data_server_call_back:
                    try:
                        self.set_temp_dir(self.par_dic['session_id'], verbose=verbose)
                    except Exception as e:
                        sentry.capture_message(f"problem creating temp directory: {e}")

                        raise InternalError("we have encountered an internal error! "
                                            "Our team is notified and is working on it. We are sorry! "
                                            "When we find a solution we will try to reach you", status_code=500)
                    if self.instrument is not None and not isinstance(self.instrument, str):
                        products_url = self.app.config.get('conf').products_url
                        bind_host = self.app.config.get('conf').bind_host
                        bind_port = self.app.config.get('conf').bind_port
                        self.instrument.parse_inputs_files(
                            par_dic=self.par_dic,
                            request=request,
                            temp_dir=self.temp_dir,
                            verbose=verbose,
                            use_scws=self.use_scws,
                            upload_dir=self.request_files_dir,
                            products_url=products_url,
                            bind_host=bind_host,
                            bind_port=bind_port,
                            request_files_dir=self.request_files_dir,
                            decoded_token=self.decoded_token,
                            sentry_dsn=self.sentry_dsn
                        )
                        self.par_dic = self.instrument.set_pars_from_dic(self.par_dic, verbose=verbose)
                        # self.update_ownership_files(uploaded_files_obj)
                # update the job_id
                if not (data_server_call_back or resolve_job_url or download_files):
                    query_status = self.par_dic['query_status']
                    self.job_id = None
                    if query_status == 'new':
                        provided_job_id = self.par_dic.get('job_id', None)
                        if provided_job_id == "": # frontend sends this
                            provided_job_id = None

                        # let's generate the definitive job_id
                        self.generate_job_id()

                        if provided_job_id is not None and self.job_id != provided_job_id:
                            raise RequestNotUnderstood((
                                    f"during query_status == \"new\", provided (unnecessarily) job_id {provided_job_id} "
                                    f"did not match self.job_id {self.job_id} computed from request"
                                ))
                    else:
                        if 'job_id' not in self.par_dic:
                            raise RequestNotUnderstood(
                                f"job_id must be present if query_status != \"new\" (it is \"{query_status}\")")

                        self.job_id = self.par_dic['job_id']

                elif download_files:
                    self.job_id = None

                if not download_files:
                    # let's set the scratch_dir with the updated job_id
                    self.set_scratch_dir(self.par_dic['session_id'], job_id=self.job_id, verbose=verbose)

                    self.log_query_progression("before move_temp_content")
                    self.move_temp_content()
                    self.log_query_progression("after move_temp_content")

                    self.set_session_logger(self.scratch_dir, verbose=verbose, config=config)

                self.config = config

            self.logger.info(f'==> found par dict {self.par_dic.keys()}')
        except APIerror:
            raise

        except Exception as e:
            self.logger.error('\033[31mexception in constructor of %s %s\033[0m', self, repr(e))
            self.logger.error("traceback: %s", traceback.format_exc())
            raise RequestNotUnderstood(f"{self} constructor failed: {e}")

        finally:
            self.logger.info("==> clean-up temporary directory")
            self.log_query_progression("before clear_temp_dir")
            self.clear_temp_dir(temp_scratch_dir=temp_scratch_dir, temp_job_id=temp_job_id)
            self.log_query_progression("after clear_temp_dir")
            
        logger.info("constructed %s:%s for data_server_call_back=%s", self.__class__, self, data_server_call_back)

    @staticmethod
    def free_up_space(app):
        token = request.args.get('token', None)

        app_config = app.config.get('conf')
        secret_key = app_config.secret_key

        output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                      required_roles=['space manager'],
                                                                      action="free_up space on the server")

        if output_code is not None:
            return make_response(output, output_code)

        current_time_secs = time.time()
        hard_minimum_folder_age_days = app_config.hard_minimum_folder_age_days
        # let's pass the minimum age the folders to be deleted should have
        soft_minimum_folder_age_days = request.args.get('soft_minimum_age_days', None)
        if soft_minimum_folder_age_days is None:
            soft_minimum_folder_age_days = app_config.soft_minimum_folder_age_days
        else:
            soft_minimum_folder_age_days = int(soft_minimum_folder_age_days)

        list_scratch_dir = sorted(glob.glob("scratch_sid_*_jid_*"), key=os.path.getmtime)
        list_scratch_dir_to_delete = []

        for scratch_dir in list_scratch_dir:
            scratch_dir_age_days = (current_time_secs - os.path.getmtime(scratch_dir)) / (60 * 60 * 24)
            if scratch_dir_age_days >= hard_minimum_folder_age_days:
                list_scratch_dir_to_delete.append(scratch_dir)
            elif scratch_dir_age_days >= soft_minimum_folder_age_days:
                analysis_parameters_path = os.path.join(scratch_dir, 'analysis_parameters.json')
                with open(analysis_parameters_path) as analysis_parameters_file:
                    dict_analysis_parameters = json.load(analysis_parameters_file)
                token = dict_analysis_parameters.get('token', None)
                token_expired = False
                if token is not None:
                    try:
                        tokenHelper.get_decoded_token(token, secret_key)
                    except jwt.exceptions.ExpiredSignatureError:
                        token_expired = True

                job_monitor_path = os.path.join(scratch_dir, 'job_monitor.json')
                with open(job_monitor_path, 'r') as jm_file:
                    monitor = json.load(jm_file)
                    job_status = monitor['status']
                    job_id = monitor['job_id']
                if job_status == 'done' and (token is None or token_expired):
                    list_scratch_dir_to_delete.append(scratch_dir)
                else:
                    incomplete_job_alert_message = f"The job {job_id} is yet to complete despite being older "\
                                                   f"than {soft_minimum_folder_age_days} days. This has been detected "\
                                                   f"while checking for deletion the folder {scratch_dir}."

                    logger.info(incomplete_job_alert_message)
                    sentry.capture_message(incomplete_job_alert_message)
            else:
                break

        pre_clean_space_stats = shutil.disk_usage(os.getcwd())
        pre_clean_available_space =  format_size(pre_clean_space_stats.free, format_returned='M')

        logger.info(f"Number of scratch folder before clean-up: {len(list_scratch_dir)}.\n"
                    f"The available amount of space is {pre_clean_available_space}")

        for d in list_scratch_dir_to_delete:
            shutil.rmtree(d)

        list_lock_files = sorted(glob.glob(".lock_*"), key=os.path.getatime)
        num_lock_files_removed = 0
        for l in list_lock_files:
            lock_file_job_id = l.split('_')[-1]
            list_job_id_scratch_dir = glob.glob(f"scratch_sid_*_jid_{lock_file_job_id}*")
            if len(list_job_id_scratch_dir) == 0:
                os.remove(l)
                num_lock_files_removed += 1

        post_clean_space_space = shutil.disk_usage(os.getcwd())
        post_clean_available_space = format_size(post_clean_space_space.free, format_returned='M')

        list_scratch_dir = sorted(glob.glob("scratch_sid_*_jid_*"))
        list_lock_files = sorted(glob.glob(".lock_*"))
        logger.info(f"Number of scratch folder after clean-up: {len(list_scratch_dir)}, "
                    f"number of lock files after clean-up: {len(list_lock_files)}.\n"
                    f"Removed {len(list_scratch_dir_to_delete)} scratch directories "
                    f"and {num_lock_files_removed} lock files.\n"
                    f"Now the available amount of space is {post_clean_available_space}")

        result_scratch_dir_deletion = f"Removed {len(list_scratch_dir_to_delete)} scratch directories, " \
                                      f"and {num_lock_files_removed} lock files."
        logger.info(result_scratch_dir_deletion)

        return jsonify(dict(output_status=result_scratch_dir_deletion))

    @staticmethod
    def get_user_specific_instrument_list(app):
        token = request.args.get('token', None)

        roles = []
        email = None
        if token is not None:
            app_config = app.config.get('conf')
            secret_key = app_config.secret_key
            output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                          action="getting the list of instrument")
            if output_code is not None:
                return make_response(output, output_code)
            else:
                decoded_token = tokenHelper.get_decoded_token(token, secret_key)
                roles = tokenHelper.get_token_roles(decoded_token)
                email = tokenHelper.get_token_user_email_address(decoded_token)

        with block_timer(logger=logger, 
                         message_template="Instrument factory iteration took {:.1f} seconds"):
            out_instrument_list = []
            for instrument_factory in importer.instrument_factory_iter:
                if hasattr(instrument_factory, 'instrument_query'):
                    instrument_query = instrument_factory.instrument_query
                    if hasattr(instrument_factory, 'instr_name'):
                        instr_name = instrument_factory.instr_name
                    else:
                        instr_name = instrument_factory().name
                else:
                    instrument = instrument_factory()
                    instrument_query = instrument.instrumet_query
                    instr_name = instrument.name
                    

                if instrument_query.check_instrument_access(roles, email):
                    out_instrument_list.append(instr_name)

        return jsonify(out_instrument_list)

    @staticmethod
    def inspect_user_state(user_email):
        return InstrumentQueryBackEnd.inspect_state(user_email)

    @staticmethod
    def inspect_state(user_email=None):
        recent_days = request.args.get('recent_days', 3, type=float)
        job_id = request.args.get('job_id', None)
        include_session_log = request.args.get('include_session_log', False) == 'True'
        include_status_query_output = request.args.get('include_status_query_output', False) == 'True'
        exclude_analysis_parameters = request.args.get('exclude_analysis_parameters', False) == 'True'
        group_by_job = request.args.get('group_by_job', False) == 'True'
        records_content = []

        for scratch_dir in glob.glob("scratch_sid_*_jid_*"):
            r = re.match(
                r"scratch_sid_(?P<session_id>[A-Z0-9]{16})_jid_(?P<job_id>[a-z0-9]{16})(?P<aliased_marker>_aliased|)",
                scratch_dir)
            if r is not None:
                if job_id is not None:
                    if r.group('job_id')[:8] != job_id:
                        continue
                scratch_dir_job_id = r.group('job_id')
                if os.path.exists(scratch_dir):
                    if (time_.time() - os.stat(scratch_dir).st_mtime) < recent_days * 24 * 3600:
                        if group_by_job:
                            result_job_status = InstrumentQueryBackEnd.read_job_status_scratch_dir(scratch_dir,
                                                                                                   include_session_log=include_session_log,
                                                                                                   include_status_query_output=include_status_query_output,
                                                                                                   exclude_analysis_parameters=exclude_analysis_parameters,
                                                                                                   user_email=user_email
                                                                                                   )
                            if result_job_status is not None:
                                job_status_search_result = [(index, job_status_obj)
                                                            for index, job_status_obj in enumerate(records_content) if
                                                            job_status_obj.get('job_id') == scratch_dir_job_id]
                                if len(job_status_search_result) > 0:
                                    records_content[job_status_search_result[0][0]]['job_status_data'].append(dict(**result_job_status))
                                else:
                                    records_content.append(dict(
                                        job_id=scratch_dir_job_id,
                                        job_status_data=[dict(**result_job_status)]
                                    )
                                    )
                        else:
                            result_content, request_completed, token_expired = InstrumentQueryBackEnd.read_content_scratch_dir(scratch_dir,
                                                                                                                               include_session_log=include_session_log,
                                                                                                                               include_status_query_output=include_status_query_output,
                                                                                                                               exclude_analysis_parameters=exclude_analysis_parameters,
                                                                                                                               user_email=user_email)

                            if result_content is not None:
                                record = dict(
                                    mtime=os.stat(scratch_dir).st_mtime,
                                    ctime=os.stat(scratch_dir).st_ctime,
                                    session_id=r.group('session_id'),
                                    job_id=scratch_dir_job_id,
                                    request_completed=request_completed,
                                    aliased_marker=r.group('aliased_marker'),
                                    **result_content
                                )
                                if token_expired is not None:
                                    record['token_expired'] = token_expired
                                records_content.append(record)
                else:
                    logger.warning(f"scratch_dir {scratch_dir} not existing, cannot be inspected")

        logger.info("found %s records", len(records_content))

        return dict(records=records_content)

    @staticmethod
    def read_analysis_parameters_scratch_dir(scratch_dir, decode_token=False):
        analysis_parameters_obj = None
        reading_output_message = ""
        fn = os.path.join(scratch_dir, 'analysis_parameters.json')
        try:
            with open(fn) as analysis_parameters_file:
                analysis_parameters_obj = json.load(analysis_parameters_file)
        except Exception as e:
            logger.warning('unable to read: %s', fn)
            reading_output_message = f'problem reading {fn}: {repr(e)}'

        if analysis_parameters_obj is not None and  decode_token and 'token' in analysis_parameters_obj:
            analysis_parameters_obj['token'] = tokenHelper.get_decoded_token(analysis_parameters_obj['token'], secret_key=None, validate_token=False)

        return analysis_parameters_obj, reading_output_message

    @staticmethod
    def read_job_status_scratch_dir(scratch_dir, include_session_log=False, include_status_query_output=False, exclude_analysis_parameters=True, user_email=None):
        result_job_status = None
        result_content, request_completed, token_expired = InstrumentQueryBackEnd.read_content_scratch_dir(scratch_dir,
                                                                                                           include_session_log=include_session_log,
                                                                                                           include_status_query_output=include_status_query_output,
                                                                                                           exclude_analysis_parameters=exclude_analysis_parameters,
                                                                                                           user_email=user_email)
        if result_content is not None:
            result_job_status = dict(
                request_completed = request_completed,
                scratch_dir_fn = scratch_dir,
                scratch_dir_content = result_content
            )

            if token_expired is not None:
                result_job_status['token_expired'] = token_expired

        return result_job_status

    @staticmethod
    def read_content_scratch_dir(scratch_dir, include_session_log=False, include_status_query_output=False, exclude_analysis_parameters=True, user_email=None):
        result_content = {}
        file_list = []
        request_completed = False
        token_expired = None
        reading_output_message = None

        analysis_parameters, reading_output_message = InstrumentQueryBackEnd.read_analysis_parameters_scratch_dir(scratch_dir,
                                                                                                                  decode_token=True)
        if analysis_parameters is None:
            analysis_parameters = reading_output_message
        else:
            if 'token' in analysis_parameters:
                token_expired = analysis_parameters['token']['exp'] < time_.time()

        if not exclude_analysis_parameters:
            result_content['analysis_parameters'] = analysis_parameters

        if user_email is not None:
            if analysis_parameters in [None, reading_output_message]:
                return None, None, None

            token = analysis_parameters.get('token', {})
            if token.get('sub') != user_email:
                return None, None, None

        for f in glob.glob(os.path.join(scratch_dir, "*")):
            file_list.append(f)
        result_content['file_list'] = file_list

        if include_session_log:
            result_content['session_log'] = ''
            session_log_fn = os.path.join(scratch_dir, 'session.log')
            if os.path.exists(session_log_fn):
                with open(session_log_fn) as session_log_fn_f:
                    result_content['session_log'] = session_log_fn_f.read()

        result_content['email_history'] = []
        for email in glob.glob(os.path.join(scratch_dir, 'email_history/*')):
            ctime = os.stat(email).st_ctime,
            result_content['email_history'].append(dict(
                ctime=ctime,
                ctime_isot=time_.strftime("%Y-%m-%dT%H:%M:%S", time_.gmtime(os.stat(email).st_ctime)),
                fn=email,
            ))

        result_content['matrix_message_history'] = []
        for msg in glob.glob(os.path.join(scratch_dir, 'matrix_message_history/*')):
            ctime = os.stat(msg).st_ctime,
            result_content['matrix_message_history'].append(dict(
                ctime=ctime,
                ctime_isot=time_.strftime("%Y-%m-%dT%H:%M:%S", time_.gmtime(os.stat(msg).st_ctime)),
                fn=msg,
            ))

        result_content['fits_files'] = []
        for fits_fn in glob.glob(os.path.join(scratch_dir, '*fits*')):
            ctime = os.stat(fits_fn).st_ctime
            result_content['fits_files'].append(dict(
                ctime=ctime,
                ctime_isot=time_.strftime("%Y-%m-%dT%H:%M:%S", time_.gmtime(ctime)),
                fn=fits_fn,
            ))

        if include_status_query_output:
            result_content['status_query_output'] = ''
            query_output_fn = os.path.join(scratch_dir, 'query_output.json')
            try:
                with open(query_output_fn) as query_output_file:
                    query_output_content = json.load(query_output_file)
                query_output_status_dict = query_output_content.get('status_dictionary', None)
                if query_output_status_dict is not None:
                    result_content['status_query_output'] = query_output_status_dict
            except Exception as e:
                logger.warning('unable to read: %s', query_output_fn)
                result_content['status_query_output'] = f'problem reading {query_output_fn}: {repr(e)}'

        result_content['job_monitor'] = []
        for fn in glob.glob(os.path.join(scratch_dir, 'job_monitor*')):
            with open(fn) as job_status_file:
                job_monitor_content = json.load(job_status_file)
            job_monitor_ctime = os.stat(fn).st_ctime
            job_monitor_status = job_monitor_content['status']
            request_completed = (request_completed or job_monitor_status == 'done')

            result_content['job_monitor'].append(dict(
                ctime=job_monitor_ctime,
                ctime_isot=time_.strftime("%Y-%m-%dT%H:%M:%S", time_.gmtime(job_monitor_ctime)),
                fn=fn,
                job_monitor_content=job_monitor_content
            ))

        return result_content, request_completed, token_expired

    @staticmethod
    def restricted_par_dic(par_dic, kw_black_list=None):
        """
        restricts parameter list to those relevant for request content
        """

        if kw_black_list is None:
            kw_black_list = default_kw_black_list

        return OrderedDict({
            k: v for k, v in par_dic.items()
            if k not in kw_black_list and v is not None
        })

    def user_specific_par_dic(self, par_dic):
        if par_dic.get('token') is not None:
            secret_key = self.app.config.get('conf').secret_key
            decoded_token = tokenHelper.get_decoded_token(par_dic['token'], secret_key)
            return {
                **par_dic,
                "sub": tokenHelper.get_token_user_email_address(decoded_token)
            }
        else:
            return par_dic

    def calculate_job_id(self,
                         par_dic: dict,
                         kw_black_list: typing.Union[None,dict]=None) -> str:
        """
        restricts parameter list to those relevant for request content, and makes string hash
        """

        user_par_dict = self.user_specific_par_dic(par_dic)
        user_restricted_par_dict = self.restricted_par_dic(user_par_dict, kw_black_list)

        return make_hash(user_restricted_par_dict)

    def generate_job_id(self, kw_black_list=None):
        self.logger.info("\033[31m---> GENERATING JOB ID <---\033[0m")
        self.logger.info(
            "\033[31m---> new job id for %s <---\033[0m", self.par_dic)

        try:
            self.logger.debug("generate_job_id: %s", json.dumps(self.par_dic, indent=4, sort_keys=True))
        except Exception as e:
            self.logger.error("unable to jsonify this self.par_dic = %s", self.par_dic)
            raise

        self.job_id = self.calculate_job_id(self.par_dic, kw_black_list)

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
        logger = logging.getLogger(__name__)

        if verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        session_log_filename = os.path.join(scratch_dir, 'session.log')

        have_handler = False
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler) and handler.baseFilename:
                handler_path = pathlib.Path(handler.baseFilename)
                if handler_path.parent.stem == scratch_dir:
                    logger.info("found correspondent FileHandler: %s : %s",
                                handler, handler.baseFilename)
                    have_handler = True
                else:
                    logger.info("found not correspondent FileHandler: %s : %s, assigning a new one",
                                handler, handler.baseFilename)
                    logger.removeHandler(handler)
                #handler.baseFilename == session_log_filename

        if not have_handler:

            fileh = logging.FileHandler(session_log_filename, 'a')
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fileh.setFormatter(formatter)

            logger.addHandler(fileh)  # set the new handler

        if verbose:
            print('logfile set to dir=', scratch_dir,
                  ' with name=', session_log_filename)

        # if config is not None:
        #    logger=self.set_logstash(logger,logstash_host=config.logstash_host,logstash_port=config.logstash_port)

        self.logger = logger

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

    def set_sentry_sdk(self, sentry_dsn=None):
        if sentry_dsn is not None:
            if sentry.sentry_url != sentry_dsn:
                raise NotImplementedError

        self.sentry_dsn = sentry.sentry_url

    def get_current_ip(self):
        return socket.gethostbyname(socket.gethostname())

    def set_scws_related_params(self, request):
        # it is nowhere necessary within the dispatcher-app,
        # but it is re-attached to the url within the email
        # when sending it since it is used by the frontend

        self.use_scws = self.par_dic.pop('use_scws', None)
        #
        if 'scw_list' in self.par_dic.keys():
            if self.use_scws == 'no' or self.use_scws == 'user_file':
                raise RequestNotUnderstood("scw_list parameter was found "
                                           "despite use_scws was indicating this was not provided, "
                                           "please check the inputs")
            _p = request.values.getlist('scw_list')
            if len(_p) > 1:
                self.par_dic['scw_list'] = _p
            # it could be a comma-separated string, so let's convert to a list
            elif len(_p) == 1:
                _p_space_separated = _p[0].split()
                if len(_p_space_separated) > 1:
                    raise RequestNotUnderstood('a space separated science windows list is an unsupported format, '
                                               'please provide it as a comme separated list')
                _p = str(_p)[1:-1].replace('\'','').split(",")
                if len(_p) > 1:
                    # TODO to be extended also to cases with one element,
                    #  so to be consistent with how it is handled when passed with a file,
                    #  EDIT: after a quick check, only a test adaptation is needed, though net very crucial
                    self.par_dic['scw_list'] = _p
            # use_scws should be set for, if any, url link within the email
            if self.use_scws is None:
                self.use_scws = 'form_list'
            print('=======> scw_list',  self.par_dic['scw_list'], _p, len(_p))
        else:
            # not necessary to check the case of scw_list passed via file,
            # since it is verified at a later stage
            if self.use_scws is not None and self.use_scws == 'form_list':
                raise RequestNotUnderstood("scw_list parameter was expected to be passed, "
                                           "but it has not been found, "
                                           "please check the inputs")
            if self.use_scws is None or self.use_scws == 'no':
                # to prevent the scw_list to be added to the par_dict
                # TODO: to be improved!
                params_not_to_be_included.append('scw_list')

    def set_scws_call_back_related_params(self):
        # get the original params dict from the json file within the folder
        original_request_par_dic = self.get_request_par_dic()
        if original_request_par_dic is not None:
            self.use_scws = original_request_par_dic.get('use_scws', None)
            if 'scw_list' in original_request_par_dic.keys():
                if self.use_scws is None:
                    self.use_scws = 'form_list'

    def set_args(self, request, verbose=False, download_products=False, download_files=False):
        supported_methods = ['GET', 'POST']
        if download_files or download_products:
            supported_methods.append('HEAD')
        if request.method in supported_methods:
            args = request.values
        else:
            raise NotImplementedError

        self.par_dic = {}
        for k, v in args.to_dict().items():
            if k in ['catalog_selected_objects', 'selected_catalog']:
                self.par_dic[k] = v
                continue
            if v == '\x00':
                self.par_dic[k] = None
                continue
            try:
                decoded = json.loads(v)
                if isinstance(decoded, (dict, list)):
                    self.par_dic[k] = decoded
                else:
                    self.par_dic[k] = v
            except json.JSONDecodeError:
                self.par_dic[k] = v
        
        if verbose:
            print('par_dic', self.par_dic)

        self.args = args

    def get_request_files_dir(self):
        request_files_dir = FilePath(file_dir='request_files')
        request_files_dir.mkdir()
        return request_files_dir.path

    def set_scratch_dir(self, session_id, job_id=None, verbose=False):
        lock_file = f".lock_{self.job_id}"
        scratch_dir_retry_attempts = 6
        scratch_dir_retry_delay = 0.2
        scratch_dir_created = True

        if verbose:
            print('SETSCRATCH  ---->', session_id, type(session_id), job_id, type(job_id))

        wd = 'scratch'

        if session_id is not None:
            wd += '_sid_' + session_id

        if job_id is not None:
            wd += '_jid_'+job_id

        for attempt in range(scratch_dir_retry_attempts):
            try:
                with open(lock_file, 'w') as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    alias_workdir = self.get_existing_job_ID_path(wd=FilePath(file_dir=wd).path)
                    if alias_workdir is not None:
                        wd = wd + '_aliased'

                    wd_path_obj = FilePath(file_dir=wd)
                    wd_path_obj.mkdir()
                    self.scratch_dir = wd_path_obj.path
                    scratch_dir_created = True
                    break
            except (OSError, IOError) as io_e:
                scratch_dir_created = False
                self.logger.warning(f'Failed to acquire lock for the scratch directory "{wd}" creation, attempt number {attempt + 1} ({scratch_dir_retry_attempts - (attempt + 1)} left), sleeping {scratch_dir_retry_delay} seconds until retry.\nError: {str(io_e)}')
                time.sleep(scratch_dir_retry_delay)
                scratch_dir_retry_delay *= 2

        if not scratch_dir_created:
            dir_list = glob.glob(f"*_jid_{job_id}*")
            sentry.capture_message(f"Failed to acquire lock for \"{wd}\" directory creation after multiple attempts.\njob_id: {self.job_id}\ndir_list: {dir_list}")
            raise InternalError(f"Failed to acquire lock for directory \"{wd}\" creation after {scratch_dir_retry_attempts} attempts.", status_code=500)

    def set_temp_dir(self, session_id, job_id=None, verbose=False):
        if verbose:
            print('SETTEMP  ---->', session_id,
                  type(session_id), job_id, type(job_id))

        suffix = ""

        if session_id is not None:
            suffix += '_sid_' + session_id

        if job_id is not None:
            suffix += '_jid_'+job_id
        temp_parent_dir = '.'
        if hasattr(self, 'scratch_dir'):
            temp_parent_dir = self.scratch_dir
        td = tempfile.mkdtemp(suffix=suffix, dir=temp_parent_dir)
        self.temp_dir = td

    def move_temp_content(self):
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir) \
                and os.path.exists(self.scratch_dir):
            for f in os.listdir(self.temp_dir):
                file_full_path = os.path.join(self.temp_dir, f)
                shutil.copy(file_full_path, self.scratch_dir)

    def clear_temp_dir(self, temp_scratch_dir=None, temp_job_id=None):
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        if temp_scratch_dir is not None and temp_scratch_dir != self.scratch_dir and os.path.exists(temp_scratch_dir):
            shutil.rmtree(temp_scratch_dir)
        if temp_job_id is not None and os.path.exists(f".lock_{temp_job_id}"):
            os.remove(f".lock_{temp_job_id}")


    @staticmethod
    def validated_download_file_path(basepath, filename, should_exist=True):
        # basic arg validation
        if "../" in filename or filename.startswith(os.sep):
            raise RequestNotAuthorized('No such file')
        
        # still explicitly check if the file is in the dir
        base_abs = os.path.realpath(basepath)
        file_abs = os.path.realpath(os.path.join(basepath, filename))

        if (os.path.commonpath([base_abs]) != os.path.commonpath([base_abs, file_abs]) 
                or (should_exist and not os.path.isfile(file_abs)) ):
            raise RequestNotAuthorized('No such file')
        return file_abs

    def verify_access_to_file(self, file_name):
        user_email = None
        if self.decoded_token is not None:
            user_email = tokenHelper.get_token_user_email_address(self.decoded_token)
        ownership_file_path = os.path.join(self.request_files_dir, f'{file_name}_ownerships.json')
        with open(ownership_file_path) as ownership_file:
            ownerships = json.load(ownership_file)
        if 'public' not in ownerships['user_emails'] and \
                ((user_email is not None and user_email not in ownerships['user_emails']) or user_email is None):
            raise RequestNotAuthorized('User cannot access the file')

    
    def prepare_download(self, file_list, file_name, return_archive=True, from_request_files_dir=False):
        if from_request_files_dir:
            origin_dir = self.request_files_dir
        else:
            origin_dir = self.scratch_dir

        file_name = file_name.replace(' ', '_')

        if hasattr(file_list, '__iter__'):
            print('file_list is iterable')
        else:
            file_list = [file_list]

        for ID, f in enumerate(file_list):
            file_list[ID] = self.validated_download_file_path(origin_dir, f)
            if from_request_files_dir:
                self.verify_access_to_file(f)

        file_dir = tempfile.mkdtemp(prefix='download_', dir='./')
        file_path = self.validated_download_file_path(file_dir, file_name, should_exist=False)

        if len(file_list) > 1:
            out_dir = file_name.replace('.tar', '')
            out_dir = out_dir.replace('.gz', '')
            tar = tarfile.open("%s" % (file_path), "w:gz")
            for name in file_list:
                if name is not None:
                    tar.add(name, arcname='%s/%s' %
                            (out_dir, os.path.basename(name)))
            tar.close()
        else:
            if return_archive:
                with open(file_list[0], "rb") as f_in:
                    in_data = f_in.read()
                with gzip.open(file_path, 'wb') as f:
                    f.write(in_data)
            else:
                file_to_download = file_list[0].split('/')[-1]
                if file_name == file_to_download:
                    file_dir = os.path.dirname(file_list[0])
                else:
                    shutil.copy(file_list[0], file_path)

        file_dir = os.path.abspath(file_dir)

        return file_dir, file_name

    def resolve_job_url(self):
        expected_pars = set(['job_id', 'session_id', 'token'])
        unexpected_pars = list(sorted(set(self.par_dic) - expected_pars))
        missing_pars = list(sorted(expected_pars - set(self.par_dic)))

        if len(unexpected_pars) > 0:
            raise RequestNotUnderstood(f"found unexpected parameters: {unexpected_pars}, expected only and only these {list(sorted(expected_pars))}")

        if len(missing_pars) > 0:
            raise RequestNotUnderstood(f"NOT found expected parameters: {missing_pars}, expected only and only these {list(sorted(expected_pars))}")

        self.par_dic.update(self.get_request_par_dic())

        # what if scw list from request overwrites that in self.par_dict?
        self.set_scws_related_params(request)

        self.validate_job_id(request_parameters_from_scratch_dir=True)

        return self.generate_products_url(
            self.app.config['conf'].products_url,
            self.par_dic
            )

    def validate_job_id(self, request_parameters_from_scratch_dir=False):
        """    
        makes sure self.job_id is compatible, for given user token, with:
        if request_parameters_from_scratch_dir:
            parameters found in directory matching job_id
        else: 
            parameters provided in this request
        """
        if self.job_id is not None:
            #request_par_dic = self.find_job_id_parameters(self.job_id)

            if request_parameters_from_scratch_dir:
                request_par_dic = self.find_job_id_parameters(self.job_id)
                if request_par_dic is None:
                    raise InvalidJobIDProvided(f"unable to find any record for {self.job_id}")
                else:
                    # if not job_resolution:
                    request_par_dic['token'] = self.token
            else:
                request_par_dic = self.par_dic

            if request_par_dic is not None:
                calculated_job_id = self.calculate_job_id(request_par_dic)

                if self.job_id != calculated_job_id:
                    debug_message = f"The provided job_id={self.job_id} does not match with the job_id={calculated_job_id} " \
                                    f"derived from the request parameters"
                    if not self.public:
                        debug_message += " for your user account email"

                    if request_parameters_from_scratch_dir:
                        debug_message += "; parameters are derived from recorded job state"
                    else:
                        debug_message += "; parameters are derived from this request"

                    restored_job_parameters = self.find_job_id_parameters(self.job_id)

                    logger.error(debug_message)
                    logger.error("parameters for self.job_id %s, recomputed as %s : %s",
                                self.job_id,
                                self.calculate_job_id(restored_job_parameters),
                                json.dumps(self.restricted_par_dic(self.user_specific_par_dic(restored_job_parameters)), sort_keys=True, indent=4))

                    logger.error("parameters for calculated_job_id %s : %s",
                                calculated_job_id,
                                json.dumps(self.restricted_par_dic(self.user_specific_par_dic(request_par_dic)), sort_keys=True, indent=4))


                    logstash_message(self.app, {'origin': 'dispatcher-call-back', 'event': 'unauthorized-user'})
                    raise RequestNotAuthorized("Request not authorized", debug_message=debug_message)
            else:
                logger.info("no previous parameters stored: allowing job_id")
                #TODO: only if it was just set
                #raise InvalidJobIDProvided(f"no record exists for job_id = {self.job_id}")

    # potentially this can be extended to support more modification of the token payload (e.g. roles)
    def update_token(self, update_email_options=False, refresh_token=False):

        if update_email_options:
            self.token = tokenHelper.update_token_email_options(self.token, self.app.config.get('conf').secret_key,
                                                                self.restricted_par_dic(self.par_dic))

        if refresh_token:
            max_refresh_interval = self.app.config.get('conf').token_max_refresh_interval
            refresh_interval = int(self.restricted_par_dic(self.par_dic).get('refresh_interval', max_refresh_interval))

            if refresh_interval > max_refresh_interval:
                debug_message = (f"The refresh interval requested exceeds the maximum allowed, please provide a value "
                                 f"which is lower than {max_refresh_interval} seconds")
                logger.error(debug_message)
                raise RequestNotAuthorized("Request not authorized", debug_message=debug_message)

            self.token = tokenHelper.refresh_token(self.token, self.app.config.get('conf').secret_key,
                                                   refresh_interval=refresh_interval)

        return self.token

    def download_file(self, from_request_files_dir=False):
        try:

            # TODO not entirely sure about these
            self.off_line = False
            self.api = False

            self.validate_job_id(request_parameters_from_scratch_dir=True)
            file_list = self.args.get('file_list').split(',')
            file_name = self.args.get('download_file_name', None)
            if file_name is None:
                file_name = file_list[0] if len(file_list) == 1 else 'download.tar.gz'
            return_archive = self.args.get('return_archive', 'True') == 'True'
            mimetype = 'application/x-gzip-compressed' if return_archive and len(file_list) == 1 else None
            # otherwise, for one file, the mimetype of the uncompressed file is determined, and gz only affects Content-Encoding
            # but Content-Encoding header isn't set if as_attachment=True

            tmp_dir, target_file = self.prepare_download(
                file_list, file_name,
                return_archive=return_archive,
                from_request_files_dir=from_request_files_dir)
            try:
                return send_from_directory(directory=tmp_dir, path=target_file, attachment_filename=target_file,
                                        as_attachment=True, mimetype=mimetype)
            except Exception as e:
                return send_from_directory(directory=tmp_dir, filename=target_file, attachment_filename=target_file,
                                           as_attachment=True, mimetype=mimetype)
        except RequestNotAuthorized as e:
            extract_job_monitor = True
            if not hasattr(self, 'scratch_dir') or self.scratch_dir is None:
                extract_job_monitor = False
            return self.build_response_failed('oda_api permissions failed',
                                              e.message,
                                              status_code=e.status_code,
                                              debug_message=e.debug_message,
                                              extract_job_monitor=extract_job_monitor)
        except Exception as e:
            return e

    def get_meta_data(self, meta_name=None):
        src_query = SourceQuery('src_query')

        l = []
        if meta_name is None:
            if 'product_type' in self.par_dic.keys():
                prod_name = self.par_dic['product_type']
            else:
                prod_name = None
            if hasattr(self, 'instrument'):
                l.append(self.instrument.get_parameters_list_jsonifiable(prod_name=prod_name))
                src_query.show_parameters_list()
            else:
                l = ['instrument not recognized']

        if meta_name == 'src_query':
            l = [src_query.get_parameters_list_jsonifiable()]
            src_query.show_parameters_list()

        if meta_name == 'instrument':
            l = [self.instrument.get_parameters_list_jsonifiable()]
            self.instrument.show_parameters_list()

        return jsonify(l)

    def get_api_par_names(self):
        _l = []
        if 'product_type' in self.par_dic.keys():
            prod_name = self.par_dic['product_type']
        else:
            prod_name = None
        if hasattr(self, 'instrument'):
            _l = self.instrument.get_arguments_name_list(prod_name=prod_name)
            if 'user_catalog' in _l:
                _l.remove('user_catalog')
        else:
            _l = ['instrument not recognized']
        return jsonify(_l)

    def get_paramters_dict(self):
        # print('CICCIO',self.par_dic)
        return jsonify(self.par_dic)

    def get_instr_list(self, name=None):
        with block_timer(logger=logger, 
                         message_template="Instrument factory iteration took {:.1f} seconds"):
            _l = []
            for instrument_factory in importer.instrument_factory_iter:
                if hasattr(instrument_factory, 'instr_name'):
                    iname = instrument_factory.instr_name
                else:
                    iname = instrument_factory().name
                _l.append(iname)

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

    def run_call_back(self, status_kw_name='action') -> typing.Tuple[str, typing.Union[QueryOutput, None]]:
        self.config, self.config_data_server = self.set_config()

        if self.config.sentry_url is not None:
            self.set_sentry_sdk(self.config.sentry_url)

        self.instrument_name = self.par_dic.get('instrument_name', '')

        # the time the request was sent should be used
        # the time_request contains the time the call_back as issued
        time_original_request = self.par_dic.get('time_original_request', None)
        job = job_factory(self.instrument_name,
                          self.scratch_dir,
                          self.dispatcher_host,
                          self.dispatcher_port,
                          self.dispatcher_callback_url_base,
                          self.par_dic['session_id'],
                          self.job_id,
                          self.par_dic,
                          token=self.token,
                          time_request=time_original_request)

        self.logger.info("%s.run_call_back with args %s", self, self.par_dic)
        self.logger.info("%s.run_call_back built job %s", self, job)

        if job.status_kw_name in self.par_dic.keys():
            status = self.par_dic[job.status_kw_name]
        else:
            status = 'unknown'

        logger.warn('-----> set status to %s', status)
        step = ''
        status_details = None
        product_type = None
        products_url = None
        email_api_code = None
        is_email_to_send = False
        is_message_to_send = False
        try:
            step = 'checking if a message can be sent via matrix'
            is_message_to_send = matrix_helper.is_message_to_send_callback(self.logger,
                                                                           status,
                                                                           time_original_request,
                                                                           self.scratch_dir,
                                                                           self.app.config['conf'],
                                                                           self.job_id,
                                                                           decoded_token=self.decoded_token)
        except matrix_helper.MultipleDoneMatrixMessage as e:
            job.write_dataserver_status(status_dictionary_value=status,
                                        full_dict=self.par_dic,
                                        matrix_message_status='attempted repeated sending of matrix message detected')
            logging.warning(f'attempted repeated sending of completion matrix message detected: {e}')
            sentry.capture_message(f'attempted repeated sending of completion matrix message detected: {e}')
        except MissingRequestParameter as e:
            job.write_dataserver_status(status_dictionary_value=status,
                                        full_dict=self.par_dic,
                                        call_back_status=f'parameter missing when checking if a message could be sent via matrix: {e.message}')
            logging.warning(f'parameter missing when checking if a message could be sent via matrix: {e.message}')

        try:
            step = 'checking if an email can be sent'
            is_email_to_send = email_helper.is_email_to_send_callback(self.logger,
                                                                      status,
                                                                      time_original_request,
                                                                      self.scratch_dir,
                                                                      self.app.config['conf'],
                                                                      self.job_id,
                                                                      decoded_token=self.decoded_token)
        except email_helper.MultipleDoneEmail as e:
            job.write_dataserver_status(status_dictionary_value=status,
                                        full_dict=self.par_dic,
                                        email_status='attempted repeated sending of completion email detected')
            logging.warning(f'attempted repeated sending of completion email detected: {e}')
            sentry.capture_message(f'attempted repeated sending of completion email detected: {e}')
        except MissingRequestParameter as e:
            job.write_dataserver_status(status_dictionary_value=status,
                                        full_dict=self.par_dic,
                                        call_back_status=f'parameter missing when checking if an email could be sent: {e.message}')
            logging.warning(f'parameter missing when checking if an email could be sent: {e.message}')

        try:
            if is_email_to_send or is_message_to_send:
                step = 'extracting the original request dictionary'
                original_request_par_dic = self.get_request_par_dic()
                step = 'extracting the product type from the original request dictionary'
                product_type = original_request_par_dic['product_type']
                # get more info regarding the status of the request
                status_details = None
                if status == 'done' and self.decoded_token is not None:
                    # set instrument
                    roles = tokenHelper.get_token_roles(self.decoded_token)
                    email = tokenHelper.get_token_user_email_address(self.decoded_token)
                    step = 'when setting the instrument'
                    self.set_instrument(self.instrument_name, roles, email)
                    # TODO to be included in a separate field, specific for the job status, and not bound to the email/matrix message
                    step = 'extracting the status details'
                    status_details = self.instrument.get_status_details(par_dic=original_request_par_dic,
                                                                        config=self.config,
                                                                        logger=self.logger)
                    
                # build the products URL and get also the original requested product
                step = 'extracting the products url'
                products_url = self.generate_products_url(self.config.products_url,
                                                          request_par_dict=original_request_par_dic)
                step = 'extracting the api code'
                email_api_code = DispatcherAPI.set_api_code(original_request_par_dic,
                                                            url=self.app.config['conf'].products_url + "/dispatch-data"
                                                            )
        except (TypeError, KeyError) as e:
            job.write_dataserver_status(status_dictionary_value=status,
                                        full_dict=self.par_dic,
                                        call_back_status=f'issue when {step}')
            logging.warning(f'issue when {step}: {e}')
            sentry.capture_message(f'issue when {step}: {e}')

        if is_message_to_send:
            time_request = time_original_request
            time_request_first_submitted = matrix_helper.get_first_submitted_matrix_message_time(self.scratch_dir)
            if time_request_first_submitted is not None:
                time_request = time_request_first_submitted

            res_content = matrix_helper.send_job_message(
                config=self.app.config['conf'],
                logger=self.logger,
                decoded_token=self.decoded_token,
                token=self.token,
                job_id=self.job_id,
                session_id=self.par_dic['session_id'],
                status=status,
                instrument=self.instrument_name,
                status_details=status_details,
                product_type=product_type,
                time_request=time_request,
                request_url=products_url,
                api_code=email_api_code,
                scratch_dir=self.scratch_dir)

            matrix_message_status_details = {
                "res_content": res_content
            }
            if status_details is not None:
                matrix_message_status_details['status_details'] = status_details

            matrix_message_status = 'matrix message sent'
            if 'res_content_token_user_failure' in res_content or len(res_content['res_content_bcc_users_failed']) >= 1:
                matrix_message_status = 'sending message via matrix failed'

            job.write_dataserver_status(status_dictionary_value=status,
                                        full_dict=self.par_dic,
                                        matrix_message_status=matrix_message_status,
                                        matrix_message_status_details=json.dumps(matrix_message_status_details))
        else:
            job.write_dataserver_status(status_dictionary_value=status, full_dict=self.par_dic)

        try:
            # TODO for a future implementation
            # self.validate_job_id()
            if is_email_to_send:
                time_request = time_original_request
                time_request_first_submitted = email_helper.get_first_submitted_email_time(self.scratch_dir)
                if time_request_first_submitted is not None:
                    time_request = time_request_first_submitted

                email_helper.send_job_email(
                    config=self.config,
                    logger=self.logger,
                    decoded_token=self.decoded_token,
                    token=self.token,
                    job_id=self.job_id,
                    session_id=self.par_dic['session_id'],
                    status=status,
                    status_details=status_details,
                    instrument=self.instrument_name,
                    product_type=product_type,
                    time_request=time_request,
                    request_url=products_url,
                    # products_url is frontend URL, clickable by users.
                    # dispatch-data is how frontend is referring to the dispatcher, it's fixed in frontend-astrooda code
                    api_code=email_api_code,
                    scratch_dir=self.scratch_dir,
                    )

                job.write_dataserver_status(status_dictionary_value=status,
                                            full_dict=self.par_dic,
                                            email_status='email sent',
                                            email_status_details=status_details)
            else:
                job.write_dataserver_status(status_dictionary_value=status, full_dict=self.par_dic)

        except email_helper.EMailNotSent as e:
            job.write_dataserver_status(status_dictionary_value=status,
                                        full_dict=self.par_dic,
                                        email_status='sending email failed')
            logging.warning(f'email sending failed: {e}')
            sentry.capture_message(f'sending email failed {e}')

        # TODO for a future implementation
        # except RequestNotAuthorized as e:
        #     job.write_dataserver_status(status_dictionary_value=status,
        #                                 full_dict=self.par_dic,
        #                                 call_back_status=f'unauthorized request detected during call back: {e.message}')
        #     if self.sentry_client is not None:
        #         self.sentry_client.capture('raven.events.Message',
        #                                message=f'attempted call_back with worng job_id {e.message}')
        #     logging.warning(f'unauthorized request detected during call back: {e.message}')

    def get_request_par_dic(self) -> dict:
        """
        returns parameters from current job/session, if those are not provided
        """
        fn = self.scratch_dir + '/analysis_parameters.json'

        if os.path.exists(fn):
            with open(fn) as file:
                return json.load(file)
        else:
            logger.info("get_request_par_dic unable to find %s", fn)

    def find_job_id_parameters(self, job_id):
        """
        returns parameters from current job and any session
        """

        scratch_dir_parameters = glob.glob(f'scratch*_jid_{job_id}*/analysis_parameters.json')
        if len(scratch_dir_parameters) == 0:
            return None
        else:
            return json.load(open(scratch_dir_parameters[0]))

    def set_use_scws(self, par_dict):
        par_dict = par_dict.copy()
        # this is a "default" value for use_scws
        par_dict['use_scws'] = 'no'
        if 'scw_list' in par_dict and self.use_scws is not None:
            # for the url, that will re-direct to the frontend,
            # the correct checkbox has to be selected,
            # then the scw_list input box will be filled correctly
            if self.use_scws == 'user_file':
                self.use_scws = 'form_list'
            # for the frontend
            par_dict['use_scws'] = self.use_scws

        return par_dict

    def generate_products_url(self, products_url, request_par_dict) -> str:
        par_dict = self.set_use_scws(request_par_dict)
        return email_helper.generate_products_url_from_par_dict(products_url, par_dict)

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

        #print ('query doen with job status-->',job_status)

        if off_line == False:
            #print('out', out_dict)
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
                                  api=False) -> QueryOutput:

        out_dict = {}

        if query_new_status is not None:
            out_dict['query_status'] = query_new_status
        if query_out is not None:
            out_dict['products'] = query_out.prod_dictionary
            out_dict['exit_status'] = query_out.status_dictionary
            if getattr(self.instrument, 'unknown_arguments_name_list', []):
                if len(self.instrument.unknown_arguments_name_list) == 1:
                    comment = f'Please note that argument {self.instrument.unknown_arguments_name_list[0]} is not used'
                else:
                    comment = f'Please note that arguments {", ".join(self.instrument.unknown_arguments_name_list)} are not used'
                out_dict['exit_status']['comment'] = \
                    out_dict['exit_status']['comment'] + ' ' + comment if out_dict['exit_status']['comment'] else comment

        if job_monitor is not None:
            out_dict['job_monitor'] = job_monitor
            out_dict['job_status'] = job_monitor['status']

        out_dict['session_id'] = self.par_dic['session_id']

        if status_code is not None:
            out_dict['status_code'] = status_code

        out_dict['time_request'] = self.time_request

        if off_line:
            return out_dict
        else:
            try:
                if api:
                    return self.jsonify_api_response(out_dict), status_code
                else:
                    return jsonify(out_dict), status_code

            except Exception as e:
                print('failed', e)
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

    def set_instrument(self, instrument_name, roles, email):

        known_instruments = []

        new_instrument = None
        no_access = False
        # TODO to get rid of the mock instrument option, we now have the empty instrument
        if instrument_name == 'mock':
            new_instrument = 'mock'
        else:
            with block_timer(logger=logger, 
                             message_template="Instrument factory iteration took {:.1f} seconds"):
                for instrument_factory in importer.instrument_factory_iter:
                    _instrument = None
                    if hasattr(instrument_factory, 'instr_name'):
                        instr_name = instrument_factory.instr_name
                    else:
                        _instrument = instrument_factory()
                        instr_name = _instrument.name
                    
                    if instr_name == instrument_name:
                        if _instrument is None and hasattr(instrument_factory, 'instrument_query'):
                            instr_query = instrument_factory.instrument_query
                        else:
                            if _instrument is None:
                                _instrument = instrument_factory()
                            instr_query = _instrument.instrumet_query
                                
                        if instr_query.check_instrument_access(roles, email):
                            if _instrument is None:
                                _instrument = instrument_factory()
                            new_instrument = _instrument  # multiple assignment? TODO
                        else:
                            no_access = True

                    known_instruments.append(instr_name)
        if new_instrument is None:
            if no_access:
                raise RequestNotAuthorized(f"Unfortunately, your priviledges are not sufficient "
                                           f"to make the request for this instrument.\n")
            else:
                raise InstrumentNotRecognized(f'instrument: "{instrument_name}", known: {known_instruments}')
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
            # print('-->',disp_data_server_conf_dict)
            if 'data_server' in disp_data_server_conf_dict.keys():
                #print (disp_data_server_conf_dict)
                if self.instrument.name in disp_data_server_conf_dict['data_server'].keys():
                    # print('-->',disp_data_server_conf_dict['data_server'][self.instrument.name].keys(),self.instrument.name)
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
        dir_list = glob.glob(f'*_jid_{self.job_id}')
        dir_list = [d for d in dir_list if 'aliased' not in d]

        if len(dir_list) == 1:
            if dir_list[0] != wd:
                alias_dir = dir_list[0]
            else:
                alias_dir = None

        elif len(dir_list) > 1:
            sentry.capture_message(f'Found two or more non aliased identical job_id, dir_list: {dir_list}')
            self.logger.warning(f'Found two or more non aliased identical job_id, dir_list: {dir_list}')
            raise InternalError("We have encountered an internal error! "
                                "Our team is notified and is working on it. We are sorry! "
                                "When we find a solution we will try to reach you",
                                status_code=500)

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
                           aliased=False,
                           token=self.token,
                           time_request=self.time_request)

    def build_response_failed(self, message, extra_message, status_code=None, debug_message='', extract_job_monitor=True):
        if extract_job_monitor:
            job = self.build_job()
            job.set_failed()
            job_monitor = job.monitor
            job_status = job_monitor['status']
        else:
            job_monitor = None
            job_status = 'failed'

        query_status = 'failed'

        query_out = QueryOutput()

        failed_task = message

        query_out.set_failed(failed_task,
                             message=extra_message,
                             job_status=job_status,
                             debug_message=debug_message)

        resp = self.build_dispatcher_response(query_new_status=query_status,
                                              query_out=query_out,
                                              job_monitor=job_monitor,
                                              status_code=status_code,
                                              off_line=self.off_line,
                                              api=self.api,)
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
            # if not self.return_progress:
            os.rename(self.response_filename, self.response_log_filename)
            self.logger.info("renamed query log log %s => %s",
                             self.response_filename, self.response_log_filename)

        if self.return_progress:
            json.dump(job_monitor, open(self.response_filename + ".return-progress-job-monitor", "w"))
        else:
            json.dump(job_monitor, open(self.response_filename + ".job-monitor", "w"))
        query_out.serialize(open(self.response_filename, "w"))

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
                self.set_sentry_sdk(config.sentry_url)

            self.config = config

    def instrument_run_query(self, product_type, job, run_asynch, query_type, verbose, dry_run, api):

        return self.instrument.run_query(product_type,
                                         self.par_dic,
                                         self,  # this will change?
                                         job,  # this will change
                                         run_asynch,
                                         out_dir=self.scratch_dir,
                                         config=self.config_data_server,
                                         query_type=query_type,
                                         logger=self.logger,
                                         sentry_dsn=self.sentry_dsn,
                                         verbose=verbose,
                                         dry_run=dry_run,
                                         api=api,
                                         decoded_token=self.decoded_token,
                                         return_progress=self.return_progress)


    def send_query_new_status_email(self,
                                product_type,
                                query_new_status,
                                time_request=None,
                                instrument_name=None,
                                status_details=None,
                                arg_par_dic=None):
        if time_request is None:
            time_request = self.time_request
        if instrument_name is None:
            instrument_name = self.instrument.name
        if arg_par_dic is None:
            arg_par_dic = self.par_dic
        products_url = self.generate_products_url(self.config.products_url,
                                                  arg_par_dic)
        email_api_code = DispatcherAPI.set_api_code(arg_par_dic,
                                                    url=os.path.join(self.config.products_url, "dispatch-data")
                                                    )
        time_request_first_submitted = email_helper.get_first_submitted_email_time(
            self.scratch_dir)
        if time_request_first_submitted is not None:
            time_request = time_request_first_submitted

        email_helper.send_job_email(
            config=self.config,
            logger=self.logger,
            decoded_token=self.decoded_token,
            token=self.token,
            job_id=self.job_id,
            session_id=self.par_dic['session_id'],
            status=query_new_status,
            status_details=status_details,
            instrument=instrument_name,
            product_type=product_type,
            time_request=time_request,
            request_url=products_url,
            api_code=email_api_code,
            scratch_dir=self.scratch_dir)


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
            # TODO still I am not entirely sure this is a good approach
            query_status = self.par_dic['query_status']
            # the idea is that, if the query is new then, there is no analysis_parameters.json file
            # otherwise, like in the case of the click of the fit button, query_status will be 'ready'
            # and so an analysis_parameters.json dump is already there
            self.validate_job_id(request_parameters_from_scratch_dir=query_status != 'new')
        except RequestNotAuthorized as e:
            return self.build_response_failed('oda_api permissions failed',
                                              e.message,
                                              status_code=e.status_code,
                                              debug_message=e.debug_message)

        try:
            query_type = self.par_dic['query_type']
            product_type = self.par_dic['product_type']
            query_status = self.par_dic['query_status']
        except KeyError as e:
            raise MissingRequestParameter(repr(e))

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

        if not self.instrument.asynch:
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
                          token=self.token,
                          time_request=self.time_request)

        job_monitor = job.monitor

        self.logger.info(
            '-----------------> query status  old is: %s', query_status)
        self.logger.info(
            '-----------------> job status before query: %s', job.status)
        self.logger.info(
            '-----------------> job_is_aliased: %s', job_is_aliased)

        #out_dict = None
        query_out = None

        # TODO if query status== ready but you get delegation
        # TODO set query status to new and ignore alias

        if job_is_aliased == True and query_status != 'ready':
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

            print('==>updated job_monitor', job_monitor['status'])

            if job_monitor['status'] == 'ready' or job_monitor['status'] == 'failed' or job_monitor['status'] == 'done':
                # NOTE in this case if job is aliased but the original has failed
                # NOTE it will be resubmitted anyhow
                print('==>aliased job status', job_monitor['status'])
                job_is_aliased = False
                job.work_dir = original_work_dir
                job_monitor = job.updated_dataserver_monitor()
                # Note this is necessary to avoid a never ending loop in the non-aliased job-status is set to progress
                print('query_status', query_status)

                query_status = 'new'
                print('==>ALIASING switched off  for status',
                      job_monitor['status'])

                if query_type == 'Dummy':
                    job_is_aliased = False
                    job.work_dir = original_work_dir
                    job_monitor = job.updated_dataserver_monitor()
                    print('==>ALIASING switched off for Dummy query')

            if job_monitor['status'] != 'done' and job_monitor['status'] != 'failed' and query_status != 'new':
                # check the last time status was updated and in case re-submit the request
                self.instrument.disp_conf = disp_conf
                try:
                    last_modified_monitor = job.get_latest_monitor_mtime()
                except FileNotFoundError as f_e:
                    return self.build_response_failed(f'Issue when requesting the latest job monitor time '
                                                      f'for the job_id {job.job_id} and session {job.session_id}.',
                                                      str(f_e))

                self.logger.info(f'last modify at the job monitor status file at {last_modified_monitor}')
                resubmit_timeout = self.app.config['conf'].resubmit_timeout
                if time_.time() - last_modified_monitor >= resubmit_timeout:
                    # re-submit
                    try:
                        self.log_query_progression("before re-submission of instrument.run_query")
                        self.logger.info('will re-submit with self.par_dic: %s', self.par_dic)
                        query_out = self.instrument_run_query(product_type,
                                                              job,
                                                              run_asynch,
                                                              query_type,
                                                              verbose,
                                                              dry_run,
                                                              api)
                        self.log_query_progression("after re-submission of instrument.run_query")
                    except RequestNotAuthorized as e:
                        return self.build_response_failed(f'permissions exception when executing job {job.job_id}',
                                                          e.message,
                                                          status_code=e.status_code,
                                                          debug_message=e.debug_message)

                    if query_out.status_dictionary['status'] == 0:
                        query_new_status = job.get_query_new_status()

                        if email_helper.is_email_to_send_run_query(self.logger,
                                                                   query_new_status,
                                                                   self.time_request,
                                                                   self.scratch_dir,
                                                                   self.job_id,
                                                                   self.app.config['conf'],
                                                                   decoded_token=self.decoded_token):
                            try:
                                self.send_query_new_status_email(product_type, query_new_status)
                                # store an additional information about the sent email
                                query_out.set_status_field('email_status', 'email sent')
                            except email_helper.EMailNotSent as e:
                                query_out.set_status_field('email_status', 'sending email failed')
                                logging.warning(f'email sending failed: {e}')
                                sentry.capture_message(f'sending email failed {e.message}')
                    else:
                        job.set_failed()
                        query_new_status = 'failed'

                    # set also the new file_path for the job object ?
                    job._set_file_path(file_name=job.file_name, work_dir=job.work_dir)
                    if query_status != query_new_status:
                        job.write_dataserver_status()

        if job_is_aliased and query_status == 'ready':
            original_work_dir = job.work_dir
            job.work_dir = alias_workdir

            job_is_aliased = False
            job.work_dir = original_work_dir
            job_monitor = job.updated_dataserver_monitor()
            self.logger.info('==>ALIASING switched off for status ready')

        if job_is_aliased:
            delta_limit = 600

            delta = delta_limit+1

            if delta > delta_limit:
                original_work_dir = job.work_dir
                job.work_dir = alias_workdir

                job_is_aliased = False
                job.work_dir = original_work_dir
                job_monitor = job.updated_dataserver_monitor()
                print('==>ALIASING switched off for delta time >%f, delta=%f' %
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
                    self.log_query_progression("before instrument.run_query")
                    self.logger.info('will run_query with self.par_dic: %s', self.par_dic)
                    query_out = self.instrument_run_query(product_type,
                                                          job,
                                                          run_asynch,
                                                          query_type,
                                                          verbose,
                                                          dry_run,
                                                          api)
                    self.log_query_progression("after instrument.run_query")                                                          
                except RequestNotAuthorized as e:
                    return self.build_response_failed(f'permissions exception when executing job {job.job_id}',
                                                      e.message,
                                                      status_code=e.status_code,
                                                      debug_message=e.debug_message)

                self.logger.info('-----------------> job status after query: %s', job.status)

                if query_out.status_dictionary['status'] == 0:
                    query_new_status = job.get_query_new_status()

                    products_url = self.generate_products_url(self.app.config.get('conf').products_url, self.par_dic)
                    email_api_code = DispatcherAPI.set_api_code(self.par_dic,
                                                                url=os.path.join(self.app.config['conf'].products_url, "dispatch-data"))

                    if matrix_helper.is_message_to_send_run_query(
                            self.logger,
                            query_new_status,
                            self.time_request,
                            self.scratch_dir,
                            self.job_id,
                            self.app.config['conf'],
                            decoded_token=self.decoded_token):

                        time_request = self.time_request
                        time_request_first_submitted = matrix_helper.get_first_submitted_matrix_message_time(self.scratch_dir)
                        if time_request_first_submitted is not None:
                            time_request = time_request_first_submitted

                        res_content = matrix_helper.send_job_message(
                            config=self.app.config['conf'],
                            logger=self.logger,
                            decoded_token=self.decoded_token,
                            token=self.token,
                            job_id=self.job_id,
                            session_id=self.par_dic['session_id'],
                            status=query_new_status,
                            instrument=self.instrument.name,
                            product_type=product_type,
                            time_request=time_request,
                            request_url=products_url,
                            api_code=email_api_code,
                            scratch_dir=self.scratch_dir)

                        matrix_message_status_details =  json.dumps({
                            "res_content": res_content
                        })

                        matrix_message_status = 'matrix message sent'
                        if 'res_content_token_user_failure' in res_content or len(res_content['res_content_bcc_users_failed']) >= 1:
                            matrix_message_status = 'sending message via matrix failed'

                        query_out.set_status_field('matrix_message_status', matrix_message_status)
                        query_out.set_status_field('matrix_message_status_details', matrix_message_status_details)

                    if email_helper.is_email_to_send_run_query(self.logger,
                                                               query_new_status,
                                                               self.time_request,
                                                               self.scratch_dir,
                                                               self.job_id,
                                                               self.app.config['conf'],
                                                               decoded_token=self.decoded_token):
                        try:

                            time_request = self.time_request
                            time_request_first_submitted = email_helper.get_first_submitted_email_time(self.scratch_dir)
                            if time_request_first_submitted is not None:
                                time_request = time_request_first_submitted

                            email_helper.send_job_email(
                                config=self.app.config['conf'],
                                logger=self.logger,
                                decoded_token=self.decoded_token,
                                token=self.token,
                                job_id=self.job_id,
                                session_id=self.par_dic['session_id'],
                                status=query_new_status,
                                instrument=self.instrument.name,
                                product_type=product_type,
                                time_request=time_request,
                                request_url=products_url,
                                api_code=email_api_code,
                                scratch_dir=self.scratch_dir)

                            # store an additional information about the sent email
                            query_out.set_status_field('email_status', 'email sent')
                        except email_helper.EMailNotSent as e:
                            query_out.set_status_field('email_status', 'sending email failed')
                            logging.warning(f'email sending failed: {e}')
                            sentry.capture_message(f'sending email failed {e.message}')

                else:
                    query_new_status = 'failed'
                    job.set_failed()
                if not self.return_progress:
                    job.write_dataserver_status()

            print('-----------------> query status update for done/ready: ',
                  query_new_status)

        elif query_status == 'progress' or query_status == 'unaccessible' or query_status == 'unknown' or query_status == 'submitted':
            # we can not just avoid async here since the request still might be long
            if self.async_dispatcher:
                query_out, job_monitor, query_new_status = self.async_dispatcher_query(query_status)
                if job_monitor is None:
                    job_monitor = job.monitor
            else:
                query_out = QueryOutput()

                job_monitor = job.updated_dataserver_monitor()

                self.logger.info('-----------------> job monitor from data server: %s', job_monitor['status'])

                if job_monitor['status'] == 'done':
                    job.set_ready()

                query_out.set_done(job_status=job_monitor['status'])

                if job_monitor['status'] in ['unaccessible', 'unknown']:
                    query_new_status = query_status
                else:
                    query_new_status = job.get_status()

            print('-----------------> job monitor updated',
                  job_monitor['status'])
            print('-----------------> query status update for progress:',
                  query_new_status)

        elif query_status == 'failed':
            # TODO: here we should resubmit query to get exception from ddosa
            query_out = QueryOutput()
            query_out.set_failed(
                'submitted job', job_status=job_monitor['status'])

            query_new_status = 'failed'
            # will send an email with the failed state
            print('-----------------> query status update for failed:',
                  query_new_status)
            print(
                '==============================> query done <==============================')

        else:
            query_out = QueryOutput()
            job_monitor = job.updated_dataserver_monitor()
            query_out.set_status(0, job_status=job_monitor['status'])

            query_new_status = job.get_status()

            self.logger.info(
                'query_out:job_monitor[status]: %s', job_monitor['status'])
            self.logger.info(
                '-----------------> query status now: %s', query_new_status)
            self.logger.info(
                '==============================> query done <==============================')

        if not job_is_aliased and query_status != query_new_status and not self.return_progress:
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

