#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May 10 10:55:20 2017

@author: andrea tramcere
"""

from builtins import (open, str, range,
                      object)
from collections import Counter, OrderedDict
import  copy
from werkzeug.utils import secure_filename

import os
import glob
import string
import  random
from raven.contrib.flask import Sentry

from flask import jsonify,send_from_directory,redirect
from flask import Flask, request
from flask.json import JSONEncoder

import tempfile
import tarfile
import gzip
import logging
import socket
import logstash

from ..plugins import importer
from ..analysis.queries import *
from ..analysis.job_manager import Job,job_factory
from ..analysis.io_helper import FilePath
from .mock_data_server import mock_query
from ..analysis.products import QueryOutput
from ..configurer import DataServerConf
from ..analysis.plot_tools import Image

import oda_api


class InstrumentQueryBackEnd(object):

    def __init__(self,app,instrument_name=None,par_dic=None,config=None,data_server_call_back=False,verbose=False,get_meta_data=False):
        #self.instrument_name=instrument_name

        self.app=app
        try:
            if par_dic is None:
                self.set_args(request,verbose=verbose)
            else:
                self.par_dic = par_dic


            self.set_session_id()
            if instrument_name is None:
                self.instrument_name = self.par_dic['instrument']
            else:
                self.instrument_name = instrument_name

            if get_meta_data==True:
                self.set_instrument(self.instrument_name)
                #TODO
                #decide if it is worth to add the logger also in this case
                #self.set_scratch_dir(self.par_dic['session_id'], verbose=verbose)
                #self.set_session_logger(self.scratch_dir, verbose=verbose, config=config)
                #self.set_sentry_client()
            else:
                if data_server_call_back is True:
                    self.job_id = self.par_dic['job_id']

                else:
                    query_status = self.par_dic['query_status']
                    self.job_id = None
                    if query_status == 'new':
                        self.generate_job_id()
                    else:
                        self.job_id = self.par_dic['job_id']

                self.set_scratch_dir(self.par_dic['session_id'],job_id=self.job_id,verbose=verbose)



                self.set_session_logger(self.scratch_dir,verbose=verbose,config=config)
                self.set_sentry_client()

                if data_server_call_back is False:
                    self.set_instrument(self.instrument_name)

                self.config=config


        except Exception as e:
            print ('e',e)



            query_out = QueryOutput()
            query_out.set_query_exception(e,'InstrumentQueryBackEnd constructor',extra_message='InstrumentQueryBackEnd constructor failed')

            #out_dict = {}
            #out_dict['query_status'] = 1
            #out_dict['exit_status'] = query_out.status_dictionary
            self.build_dispatcher_response(query_new_status='failed',query_out=query_out)


            #return jsonify(out_dict)



    def make_hash(self,o):

        """
        Makes a hash from a dictionary, list, tuple or set to any level, that contains
        only other hashable types (including any lists, tuples, sets, and
        dictionaries).
        """

        if isinstance(o, (set, tuple, list)):
            #print('o',o)
            return tuple([self.make_hash(e) for e in o])

        elif not isinstance(o, dict):
            #print('o', o)
            return hash(o)

        new_o = copy.deepcopy(o)
        for k, v in new_o.items():
            #if k not in kw_black_list:
            #    print('k',k)
            new_o[k] = self.make_hash(v)

        return u'%s'%hash(tuple(frozenset(sorted(new_o.items()))))


    def generate_job_id(self,kw_black_list=['session_id']):
        print("---> GENERATING JOB ID <---")

        #TODO generate hash (immutable ore convert to Ordered): DONE
        #import collections

        #self.par_dic-> collections.OrderedDict(self.par_dic)
        #oredered_dict=OrderedDict(self.par_dic)

        #self.job_id=u''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))
        #print('dict',self.par_dic)

        _dict=copy.deepcopy(self.par_dic)
        for k in kw_black_list:
            _dict.pop(k)
        self.job_id=u'%s'%(self.make_hash(OrderedDict(_dict)))
        print('generated job_id',self.job_id)

    def set_session_id(self):
        print("---> GENERATING SESSION ID <---")
        if 'session_id' not in self.par_dic.keys():
            self.par_dic['session_id']=None
        print('passed SESSION ID', self.par_dic['session_id'])

        if self.par_dic['session_id'] is None or self.par_dic['session_id']=='new':
            self.par_dic['session_id']=u''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))

        print('generated SESSION ID',self.par_dic['session_id'])
        print('-------')
    def set_session_logger(self,scratch_dir,verbose=False,config=None):
        logger = logging.getLogger(__name__)
        fileh = logging.FileHandler(os.path.join(scratch_dir, 'session.log'), 'a')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fileh.setFormatter(formatter)

        log = logging.getLogger()  # root logger
        for hdlr in log.handlers[:]:  # remove all old handlers
            log.removeHandler(hdlr)
        log.addHandler(fileh)  # set the new handler
        logger.setLevel(logging.INFO)



        if verbose==True:
            print('logfile set to dir=', scratch_dir, ' with name=session.log')

        if config is not None:
            logger=self.set_logstash(logger,logstash_host=config.logstash_host,logstash_port=config.logstash_port)

        self.logger=logger

    def set_logstash(self,logger,logstash_host=None,logstash_port=None):
        _logger=logger
        if logstash_host is not None:
            logger.addHandler(logstash.TCPLogstashHandler(logstash_host, logstash_port))

            extra = {
                'origin': 'cdici_dispatcher',
            }
            _logger = logging.LoggerAdapter(logger, extra)
        else:
            pass

        return _logger




    def set_sentry_client(self,sentry_url=None):

        if sentry_url is not None:
            from raven import Client

            client=     Client(sentry_url)
        else:
            client=None


        self.sentry_client=client

    def get_current_ip(self):
        return  socket.gethostbyname(socket.gethostname())

    def set_args(self,request,verbose=False):
        if request.method == 'GET':
            args = request.args
        if request.method == 'POST':
            args = request.form
        self.par_dic = args.to_dict()

        if verbose == True:
            print('par_dic', self.par_dic)

        if 'scw_list' in self.par_dic.keys():
            _p= request.args.getlist('scw_list')
            if len(_p)>1:
                self.par_dic['scw_list']=_p
            print('=======> scw_list',  self.par_dic['scw_list'],_p,len(_p))

        self.args=args

    def set_scratch_dir(self,session_id,job_id=None,verbose=False):
        if verbose==True:
            print('SETSCRATCH  ---->', session_id,type(session_id),job_id,type(job_id))

        wd = 'scratch'

        if session_id is not None:
            wd += '_sid_' + session_id


        if job_id is not None:
            wd +='_jid_'+job_id

        alias_workidr = self.get_existing_job_ID_path(wd=FilePath(file_dir=wd).path)
        if alias_workidr is not None:
            wd=wd+'_aliased'

        wd=FilePath(file_dir=wd)
        wd.mkdir()
        self.scratch_dir=wd.path


    def prepare_download(self,file_list, file_name, scratch_dir):

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
        out_dir=file_name.replace('.tar','')
        out_dir=out_dir.replace('.gz','')


        if len(file_list) > 1:
            #print('preparing tar')
            tar = tarfile.open("%s" % (file_path), "w:gz")
            for name in file_list:
                #print('add to tar', file_name,name)
                if name is not None:
                    tar.add(name,arcname='%s/%s'%(out_dir,os.path.basename(name)))
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

        tmp_dir, target_file = self.prepare_download(file_list, file_name, self.scratch_dir)
        #print('downlaoding scratch dir', self.scratch_dir)
        try:
            return send_from_directory(directory=tmp_dir, filename=target_file, attachment_filename=target_file,
                                       as_attachment=True)
        except Exception as e:
            return e

    def upload_file(self,name, scratch_dir):
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
            #print('scratch_dir',scratch_dir)
            #print('secure_file_name', filename)
            file_path = os.path.join(scratch_dir, filename)
            file.save(file_path)
            # return redirect(url_for('uploaded_file',
            #                        filename=filename))
            return file_path

    def get_meta_data(self,meta_name=None):
        src_query = SourceQuery('src_query')

        l = []
        if meta_name is None:
            #l.append(src_query.get_parameters_list_as_json())
            if 'product_type' in  self.par_dic.keys():
                prod_name = self.par_dic['product_type']
            else:
                prod_name=None

            l.append(self.instrument.get_parameters_list_as_json(prod_name=prod_name))
            src_query.show_parameters_list()

        if meta_name == 'src_query':
            l = [src_query.get_parameters_list_as_json()]
            src_query.show_parameters_list()

        if meta_name == 'instrument':
            l = [self.instrument.get_parameters_list_as_json()]
            self.instrument.show_parameters_list()

        return jsonify(l)

    def get_api_par_names(self):

        if 'product_type' in self.par_dic.keys():
            prod_name = self.par_dic['product_type']
        else:
            prod_name = None

        _l = self.instrument.get_parameters_name_list(prod_name=prod_name)
        if 'user_catalog' in _l:
            _l.remove('user_catalog')

        return jsonify(_l)


    def get_paramters_dict(self):
        #print('CICCIO',self.par_dic)
        return jsonify(self.par_dic)

    def get_instr_list(self,name=None):
        _l=[]
        for instrument_factory in importer.instrument_facotry_list:
            _l.append(instrument_factory().name)

        return jsonify(_l)


    def run_call_back(self,status_kw_name='action'):

        try:
            config, config_data_server = self.set_config()
            #print('dispatcher port', config.dispatcher_port)
        except Exception as e:
            query_out = QueryOutput()
            query_out.set_query_exception(e, 'run_query failed in %s'%self.__class__.__name__,
                                          extra_message='configuration failed')


        job = job_factory(self.par_dic['instrument_name'],
                          self.scratch_dir,
                          self.get_current_ip(),
                          config.dispatcher_port,
                          self.par_dic['session_id'],
                          self.job_id,
                          self.par_dic)


        #if 'node_id' in self.par_dic.keys():
        #    print('node_id', self.par_dic['node_id'])
        #else:
        #    print('No! node_id')

        if job.status_kw_name in self.par_dic.keys():
            status=self.par_dic[job.status_kw_name]
        else:
            status='unknown'
        #print ('-----> set status to ',status)

        job.write_dataserver_status(status_dictionary_value=status,full_dict=self.par_dic)

        return status

    def run_query_mock(self, off_line=False):





        #job_status = self.par_dic['job_status']
        session_id=self.par_dic['session_id']

        if 'instrumet' in self.par_dic.keys():
            self.par_dic.pop('instrumet')



        self.logger.info('instrument %s' % self.instrument_name)
        self.logger.info('parameters dictionary')

        for key in self.par_dic.keys():
            log_str = 'parameters dictionary, key=' + key + ' value=' + str(self.par_dic[key])
            self.logger.info(log_str)



        out_dict=mock_query(self.par_dic,session_id,self.job_id,self.scratch_dir)

        self.logger.info('============================================================')
        self.logger.info('')

        #print ('query doen with job status-->',job_status)

        if off_line == False:
            #print('out', out_dict)
            response= jsonify(out_dict)
        else:
            response= out_dict


        return response



    def build_dispatcher_response(self,query_new_status=None,query_out=None,job_monitor=None,off_line=True,api=False):


        out_dict={}

        if query_new_status is not None:
            out_dict['query_status'] = query_new_status
        if query_out is not None:
            out_dict['products'] = query_out.prod_dictionary
            out_dict['exit_status'] = query_out.status_dictionary

        if job_monitor is not None:
            out_dict['job_monitor'] = job_monitor
            out_dict['job_status'] = job_monitor['status']


        #print('exit_status', out_dict['exit_status'])

        if job_monitor is not None:
            out_dict['job_monitor'] = job_monitor
            #print('query_out:job_monitor', job_monitor)

        out_dict['session_id'] = self.par_dic['session_id']

        #print ('offline',off_line)
        if off_line == True:

            return out_dict
        else:


            try:
                #return jsonify(out_dict)
                if api == True:
                    return self.jsonify_api_response(out_dict)
                else:
                    return jsonify(out_dict)

            except Exception as e:
                print ('failed',e)
                if query_out is None:
                    query_out = QueryOutput()
                else:
                    pass

                query_out.set_failed('build dispatcher response', extra_message='failed json serialization', debug_message=str(e.message))
                out_dict['exit_status'] = query_out.status_dictionary



                return jsonify(out_dict)

    def  jsonify_api_response(self,out_dict):
        #print ('out_dict.keys()',out_dict.keys())

        if 'numpy_data_product_list' in out_dict['products']:
            _npdl=out_dict['products']['numpy_data_product_list']
            #out_dict['products']['numpy_data_product_list']=[]

            out_dict['products']['numpy_data_product_list']=[_d.encode() for _d in _npdl ]
            #print ( 'ECCO',out_dict['products']['numpy_data_product_list'],_p,_npdl)
            out_dict = jsonify(out_dict)
        else:
            out_dict = jsonify(out_dict)

        return out_dict

    def set_instrument(self, instrument_name):
        new_instrument=None
        if instrument_name == 'mock':
            new_instrument = 'mock'

        else:
            for instrument_factory in importer.instrument_facotry_list:
                instrument = instrument_factory()
                if instrument.name == instrument_name:
                    #print('setting instr',instrument_name,instrument.name)
                    new_instrument = instrument




        if new_instrument is None:

            raise Exception("instrument not recognized".format(instrument_name))
        else:
            self.instrument=new_instrument

    def set_config(self):
        if self.config is None:
            config = self.app.config.get('conf')
        else:
            config = self.config

        disp_data_server_conf_dict=config.get_data_server_conf_dict(self.instrument_name)

        #print ('--> App configuration for:',self.instrument_name)
        if disp_data_server_conf_dict is not None:
            #print('-->',disp_data_server_conf_dict)
            if 'data_server' in  disp_data_server_conf_dict.keys():
                #print (disp_data_server_conf_dict)
                if self.instrument.name in  disp_data_server_conf_dict['data_server'].keys():
                    #print('-->',disp_data_server_conf_dict['data_server'][self.instrument.name].keys(),self.instrument.name)
                    for k in disp_data_server_conf_dict['data_server'][self.instrument.name].keys():
                        if k in self.instrument.data_server_conf_dict.keys():
                            self.instrument.data_server_conf_dict[k] = disp_data_server_conf_dict['data_server'][self.instrument.name][k]

            config_data_server=DataServerConf.from_conf_dict(self.instrument.data_server_conf_dict)
        else:
            config_data_server = None
        #if hasattr(self,'instrument'):
            #config_data_server=DataServerConf.from_conf_dict(self.instrument.data_server_conf_dict)


        return config,config_data_server

    def get_existing_job_ID_path(self,wd):
        #exist same job_ID, different session ID
        dir_list=glob.glob('*_jid_%s'%(self.job_id))
        #print('dirs',dir_list)
        if dir_list !=[]:
            dir_list=[d for d in dir_list if 'aliased' not in d]

        if len(dir_list)==1:
            if dir_list[0]!=wd:
                alias_dir= dir_list[0]
            else:
                alias_dir=None

        elif len(dir_list)>1:
            raise  RuntimeError('found two non aliased identical job_id')

        else:
            alias_dir = None

        return alias_dir

    def get_file_mtime(self,file):
        return os.path.getmtime(file)

    def run_query(self,off_line=False,disp_conf=None):

        print ('==============================> run query <==============================')
        print('self.par_dic.keys()',self.par_dic.keys())
        if 'api' in self.par_dic.keys():
            #print('API version check', current_disp_oda_api_version, query_oda_api_version, failed_task, oda_api_version_error)
            print('1')
            api = True
            current_disp_oda_api_version = None
            if hasattr(oda_api, '__version__'):
                current_disp_oda_api_version = oda_api.__version__
            query_oda_api_version = None
            if 'oda_api_version' in self.par_dic.keys():
                query_oda_api_version = self.par_dic['oda_api_version']

            oda_api_version_error = None
            failed_task = 'oda_api version compatibility'

            print('2')
            print(current_disp_oda_api_version, query_oda_api_version)
            if query_oda_api_version is None:
                print('a')
                oda_api_version_error = 'oda_api version compatibility non safe, please update your oda_api package'
            elif  current_disp_oda_api_version is None:
                print('b')
                oda_api_version_error = 'oda_api on server are outdated please contact oda api responsible'
            elif current_disp_oda_api_version > query_oda_api_version:
                print('c')
                oda_api_version_error = 'oda_api version not compatible, min=%s, current=%s' % (_min_v, current_disp_oda_api_version)
            else:
                pass
            print('3')
            if oda_api_version_error is not None:
                query_status = 'failed'
                query_out = QueryOutput()
                query_out.set_failed(failed_task, message=oda_api_version_error, job_status='failed')

                resp = self.build_dispatcher_response(query_new_status=query_status, query_out=query_out, job_monitor=None, off_line=off_line, api=api)
                return resp
            print('4')
            print('API version check',current_disp_oda_api_version,query_oda_api_version,failed_task,oda_api_version_error)
        else:
            api=False

        try:
            query_type = self.par_dic['query_type']
            product_type = self.par_dic['product_type']
            query_status=self.par_dic['query_status']

        except Exception as e:
            query_out = QueryOutput()
            query_out.set_query_exception(e, 'run_query failed in %s'%self.__class__.__name__,
                                          extra_message='InstrumentQueryBackEnd constructor failed')

        print('==> query_status  ', query_status)
        if 'instrumet' in  self.par_dic.keys():
            self.par_dic.pop('instrumet')

        verbose=False
        if 'verbose' in self.par_dic.keys():
            if self.par_dic['verbose']=='True':
                verbose=True
            else:
                verbose=False

        dry_run=False
        if 'verbose' in self.par_dic.keys():
            if self.par_dic['dry_run']=='True':
                dry_run=True
            else:
                dry_run=False


        self.logger.info('product_type %s' % product_type)
        self.logger.info('query_type %s ' % query_type)
        self.logger.info('instrument %s' % self.instrument_name)
        self.logger.info('parameters dictionary')

        for key in self.par_dic.keys():
            log_str = 'parameters dictionary, key=' + key + ' value=' + str(self.par_dic[key])
            self.logger.info(log_str)

        try:

            config, config_data_server=self.set_config()
            print ('c',config,config_data_server)
            print('dispatcher port', config.dispatcher_port)
        except Exception as e:
            query_out = QueryOutput()
            query_out.set_query_exception(e, 'run_query failed in %s'%self.__class__.__name__,
                                          extra_message='configuration failed')

            config, config_data_server = None, None
        else:
            if config.sentry_url is not None:
                self.set_sentry_client(config.sentry_url)

        alias_workidr=None
        try:
            alias_workidr = self.get_existing_job_ID_path(self.scratch_dir)
        except Exception as e:
            query_out = QueryOutput()
            query_out.set_query_exception(e, 'run_query failed in s%' % self.__class__.__name__,
                                          extra_message='job aliasing failed')

        job_is_aliased = False
        run_asynch = True

        if 'run_asynch' in self.par_dic.keys():
            if self.par_dic['run_asynch']=='True':
                run_asynch=True

            elif self.par_dic['run_asynch']=='False':
                run_asynch=False
            else:
                raise  RuntimeError('run_asynch can be True or False, found',self.par_dic['run_asynch'])


        if self.instrument.asynch==False:
            run_asynch=False

        if alias_workidr is not None and run_asynch==True:
            job_is_aliased = True

        print ('--> job aliased',job_is_aliased)
        job=job_factory(self.instrument_name,
                        self.scratch_dir,
                        self.get_current_ip(),
                        config.dispatcher_port,
                        self.par_dic['session_id'],
                        self.job_id,
                        self.par_dic,
                        aliased=job_is_aliased)

        job_monitor=job.monitor



        print('-----------------> query status  old is: ',query_status )
        print('-----------------> job status before query:', job.status)
        print('-----------------> job_is_aliased:', job_is_aliased)
        out_dict=None
        query_out=None



        # TODO if query status== ready but you get delegation
        # TODO set query status to new and ignore alias



        if job_is_aliased==True and query_status!='ready':
                #print('job_is_aliased == True and query_status != ready ')
                job_is_aliased=True

                original_work_dir=job.work_dir
                job.work_dir=alias_workidr

                print ('==>ALIASING to ',alias_workidr)

                try:
                    job_monitor = job.updat_dataserver_monitor()
                except:
                    job_is_aliased=False
                    job_monitor = {}
                    job_monitor['status'] = 'failed'

                print ('==>updated job_monitor',job_monitor['status'])
                if job_monitor['status']=='ready' or  job_monitor['status']=='failed' or job_monitor['status']=='done':
                    # NOTE in this case if job is aliased but the original has failed
                    # NOTE it will be resubmitted anyhow
                    print('==>aliased job status', job_monitor['status'])
                    job_is_aliased=False
                    job.work_dir=original_work_dir
                    job_monitor = job.updat_dataserver_monitor()
                    #Note this is necessary to avoid a never ending loop in the non-aliased job-status is set to progress
                    print('query_status',query_status)
                    query_status='new'
                    print('==>ALIASING switched off  for status',job_monitor['status'])



                if query_type=='Dummy':
                    job_is_aliased = False
                    job.work_dir = original_work_dir
                    job_monitor = job.updat_dataserver_monitor()
                    print('==>ALIASING switched off for Dummy query')



        if job_is_aliased == True and query_status == 'ready':
            #print ('job_is_aliased == True and query_status ==ready ')
            original_work_dir = job.work_dir
            job.work_dir = alias_workidr

            job_is_aliased = False
            job.work_dir = original_work_dir
            job_monitor = job.updat_dataserver_monitor()
            print('==>ALIASING switched off for status ready')
            #print('==>IGNORING ALIASING to ', alias_workidr)




        if job_is_aliased == True :
            delta_limit=600
            try:
                delta = self.get_file_mtime(alias_workidr + '/' + 'job_monitor.json') - time.time()
            except:
                delta=delta_limit+1

            if delta>delta_limit:
                original_work_dir = job.work_dir
                job.work_dir = alias_workidr

                job_is_aliased = False
                job.work_dir = original_work_dir
                job_monitor = job.updat_dataserver_monitor()
                print('==>ALIASING switched off for delta time >%f, delta=%f'%(delta_limit,delta))


        print('==> aliased is', job_is_aliased)
        print('==> alias  work dir ', alias_workidr)
        print('==> job  work dir ',job.work_dir)
        print('==> query_status  ', query_status)

        if (query_status=='new'and job_is_aliased==False ) or query_status=='ready' :




            print ('*** run_asynch',run_asynch)
            print ('*** api', api)
            #if disp_conf is not None:
            #    print('ECCOLO',disp_conf.products_url)
            print ('config_data_server',config_data_server )
            self.instrument.disp_conf=disp_conf
            query_out = self.instrument.run_query(product_type,
                                                    self.par_dic,
                                                    request,
                                                    self,
                                                    job,
                                                    run_asynch,
                                                    out_dir=self.scratch_dir,
                                                    config=config_data_server,
                                                    query_type=query_type,
                                                    logger=self.logger,
                                                    sentry_client=self.sentry_client,
                                                    verbose=verbose,
                                                    dry_run=dry_run,
                                                    api=api)

            #print('-->', query_out.status_dictionary)
            #NOTE job status is set in  cdci_data_analysis.analysis.queries.ProductQuery#get_query_products
            print('-----------------> job status after query:', job.status)


            if query_out.status_dictionary['status']==0:
                if job.status=='done':
                    query_new_status='done'
                elif job.status=='failed':
                    query_new_status='failed'
                else:
                    query_new_status = 'submitted'
                    job.set_submitted()
            else:
                query_new_status = 'failed'
                job.set_failed()

            job.write_dataserver_status()

            print('-----------------> query status update for done/ready: ', query_new_status)

        elif query_status=='progress' or query_status=='unaccessible' or query_status=='unknown' or query_status=='submitted':
            query_out = QueryOutput()

            job_monitor = job.updat_dataserver_monitor()


            print('-----------------> job monitor from data server', job_monitor['status'])
            if job_monitor['status']=='done':
                job.set_ready()

            query_out.set_done(job_status=job_monitor['status'])

            if  job_monitor['status']=='unaccessible' or job_monitor['status']=='unknown':
                query_new_status=query_status
            else:

                query_new_status = job.get_status()

            print('-----------------> job monitor updated', job_monitor['status'])



            print('-----------------> query status update for progress:', query_new_status)



            print('==============================> query done <==============================')


        elif query_status=='failed':
            #TODO: here we should resubmit query to get exception from ddosa
            query_out = QueryOutput()
            query_out.set_failed('submitted job',job_status=job_monitor['status'])

            query_new_status =  'failed'
            print('-----------------> query status update for failed:', query_new_status)


            print('-----------------> query status new:', query_new_status)
            print('==============================> query done <==============================')


        else:
            query_out = QueryOutput()
            query_out.set_status(0,job_status=job_monitor['status'])

            query_new_status = job.get_status()

            print('query_out:job_monitor[status]', job_monitor['status']    )
            print('-----------------> query status new:', query_new_status)
            print('==============================> query done <==============================')

        if job_is_aliased == False:
            job.write_dataserver_status()




        self.logger.info('============================================================')
        self.logger.info('')

        resp = self.build_dispatcher_response(query_new_status=query_new_status,
                                              query_out=query_out,
                                              job_monitor=job_monitor,
                                              off_line=off_line,
                                              api=api)

        print('==============================> query done <==============================')

        return resp