#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May 10 10:55:20 2017

@author: andrea tramcere
"""

from __future__ import absolute_import, division, print_function

from builtins import (bytes, open, str, super, range,
                      zip, round, input, int, pow, object, map, zip)


from flask import Flask, request, redirect, url_for,flash
from werkzeug.utils import secure_filename

import numpy as np
import os
import random
import string

from flask import jsonify,send_from_directory
from flask import Flask, request,url_for
from flask import render_template
from flask.views import View

#from pathlib import Path
#from flask_restful import reqparse

from ..ddosa_interface.osa_isgri import OSA_ISGRI
from ..ddosa_interface.osa_jemx import OSA_JEMX
from ..analysis.queries import *
from ..analysis.job_manager import Job

from .mock_data_server import mock_query
from .mock_data_server import mock_chek_job_status
import  tempfile
import tarfile
import gzip
import logging
import socket
import threading
import sys



# from ..ddosa_interface.osa_spectrum_dispatcher import  OSA_ISGRI_SPECTRUM
#from ..ddosa_interface.osa_lightcurve_dispatcher import OSA_ISGRI_LIGHTCURVE

from ..web_display import draw_dummy

#UPLOAD_FOLDER = '/path/to/the/uploads'
#ALLOWED_EXTENSIONS = set(['txt', 'fits', 'fits.gz'])

app = Flask(__name__)
#app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


#def allowed_file(filename):
#    return '.' in filename and \
#           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS



# def run_app_threaded(conf,debug=False):
#     threaded_server=threading.Thread(target=run_app,args=(conf),kwargs={'debug':debug})
#     try:
#         # Start the server
#         threaded_server.start()
#     except Exception as ex:
#         print('flask thread failed',ex.message)
#     finally:
#
#         # Stop all running threads
#         threaded_server._Thread__stop()
#         product_dictionary={}
#         product_dictionary['error_message'] = 'flask thread failed'
#         product_dictionary['status'] = -1
#
#         return jsonify(product_dictionary)



def make_dir(out_dir):


    if os.path.isdir(out_dir):
        return
    else:
        if os.path.isfile(out_dir):
            raise RuntimeError("a file with the same name of dir already exists")
            #raise RuntimeError, "a file with the same name of dir=%s, exists"%out_dir
        else:
            os.mkdir(out_dir)







class InstrumentQueryBackEnd(object):

    def __init__(self,instrument_name=None,par_dic=None,config=None,data_server_call_back=False):
        #self.instrument_name=instrument_name

        if par_dic is None:
            self.set_args(request)
        else:
            self.par_dic = par_dic

        if instrument_name is None:
            self.instrument_name = self.par_dic['instrument']
        else:
            self.instrument_name = instrument_name



        if data_server_call_back is True:
            self.job_id = self.par_dic['job_id']

        else:
            query_status = self.par_dic['query_status']
            self.job_id = None
            if query_status == 'new':
                self.generate_job_id()
            else:
                self.job_id = self.par_dic['job_id']

        self.set_scratch_dir(self.par_dic['session_id'],job_id=self.job_id)

        self.set_session_logger(self.scratch_dir)

        if data_server_call_back is False:
            self.set_instrument(self.instrument_name)

        self.config=config


    def generate_job_id(self):
        #self.job_id=str(u''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16)))
        number = '0123456789'
        alpha = 'abcdefghijklmnopqrstuvwxyz'.capitalize()
        ID = ''
        for i in range(0, 16, 2):
            ID += random.choice(number)
            ID += random.choice(alpha)
        self.job_id=ID
        print ('------->str check',type(self.job_id),self.job_id)


    def set_instrument(self,instrument_name):
        if instrument_name == 'isgri':
            self.instrument = OSA_ISGRI()
        elif instrument_name=='jemx':
            self.instrument=OSA_JEMX()
        elif instrument_name=='mock':
            self.instrument='mock'
        else:
            raise Exception("instrument not recognized".format(instrument_name))

    def set_session_logger(self,scratch_dir):
        logger = logging.getLogger(__name__)
        fileh = logging.FileHandler(os.path.join(scratch_dir, 'session.log'), 'a')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fileh.setFormatter(formatter)

        log = logging.getLogger()  # root logger
        for hdlr in log.handlers[:]:  # remove all old handlers
            log.removeHandler(hdlr)
        log.addHandler(fileh)  # set the new handler
        logger.setLevel(logging.INFO)
        print('logfile set to dir=', scratch_dir, ' with name=session.log')

        self.logger=logger


    def get_current_ip(self):
        return  socket.gethostbyname(socket.gethostname())

    def set_args(self,request):
        if request.method == 'GET':
            args = request.args
        if request.method == 'POST':
            args = request.form
        self.par_dic = args.to_dict()
        print('par_dic', self.par_dic)

        self.args=args

    def set_scratch_dir(self,session_id,job_id=None):
        print('SETSCRATCH  ---->', session_id,type(session_id),job_id,type(job_id))
        wd = 'scratch'
        if session_id is not None:
            wd += '_' + session_id


        if job_id is not None:
            wd +='_'+job_id

        make_dir(wd)
        self.scratch_dir=wd

    def prepare_download(self,file_list, file_name, scratch_dir):
        if hasattr(file_list, '__iter__'):
            print('file_list is iterable')
        else:
            file_list = [file_list]

        for ID, f in enumerate(file_list):
            file_list[ID] = os.path.join(scratch_dir + '/', f)

        tmp_dir = tempfile.mkdtemp(prefix='download_', dir='./')
        print('using tmp dir', tmp_dir)

        file_path = os.path.join(tmp_dir, file_name)
        print('writing to file path', file_path)

        if len(file_list) > 1:
            print('preparing tar')
            tar = tarfile.open("%s" % (file_path), "w:gz")
            for name in file_list:
                print('add to tar', name)
                if name is not None:
                    tar.add(name)
            tar.close()
        else:
            print('single fits file')
            in_data = open(file_list[0], "rb").read()
            with gzip.open(file_path, 'wb') as f:
                f.write(in_data)

        tmp_dir = os.path.abspath(tmp_dir)

        return tmp_dir, file_name

    def download_products(self,):
        print('in url file_list', self.args.get('file_list'))
        #scratch_dir, logger = set_session(self.args.get('session_id'))

        file_list = self.args.get('file_list').split(',')
        print('used file_list', file_list)
        file_name = self.args.get('file_name')

        tmp_dir, target_file = self.prepare_download(file_list, file_name, self.scratch_dir)
        print('tmp_dir,target_file', tmp_dir, target_file)
        try:
            return send_from_directory(directory=tmp_dir, filename=target_file, attachment_filename=target_file,
                                       as_attachment=True)
        except Exception as e:
            return str(e)

    def upload_file(self,name, scratch_dir):
        print('upload  file')
        print('name', name)
        print('request.files ',request.files)
        if name not in request.files:
            return None
        else:
            file = request.files[name]
            print('type file', type(file))
            # if user does not select file, browser also
            # submit a empty part without filename
            if file.filename == '' or file.filename is None:
                return None

            filename = secure_filename(file.filename)
            print('scratch_dir',scratch_dir)
            print('secure_file_name', filename)
            file_path = os.path.join(scratch_dir, filename)
            file.save(file_path)
            # return redirect(url_for('uploaded_file',
            #                        filename=filename))
            return file_path

    def get_meta_data(self,name=None):
        src_query = SourceQuery('src_query')

        l = []
        if name is None:
            l.append(src_query.get_parameters_list_as_json())
            l.append(self.instrument.get_parameters_list_as_json())

        if name == 'src_query':
            l = [src_query.get_parameters_list_as_json()]

        if name == 'instrument':
            l = [self.instrument.get_parameters_list_as_json()]

        return jsonify(l)



    def run_call_back(self,status_kw_name='action'):

        if self.config is None:
            config = app.config.get('osaconf')
        else:
            config = self.config

        job = Job(work_dir=self.scratch_dir,
                  server_url=self.get_current_ip(),
                  server_port=config.dispatcher_port,
                  callback_handle='call_back',
                  session_id=self.par_dic['session_id'],
                  job_id=self.par_dic['job_id'])


        status=self.par_dic[status_kw_name]
        print ('-----> set status to ',status)
        job.write_dataserver_status(work_dir=self.scratch_dir,status_dictionary_value=status)

        return status

    def run_query_mock(self, off_line=False):


        # JOBID=PID+RAND


        job_status = self.par_dic['job_status']
        session_id=self.par_dic['session_id']


        if self.par_dic.has_key('instrumet'):
            self.par_dic.pop('instrumet')
        # prod_dictionary = self.instrument.set_pars_from_from(par_dic)

        # if prod_dictionary['status'] == 0:


        self.logger.info('instrument %s' % self.instrument_name)
        self.logger.info('parameters dictionary')

        for key in self.par_dic.keys():
            log_str = 'parameters dictionary, key=' + key + ' value=' + str(self.par_dic[key])
            self.logger.info(log_str)

        if self.config is None:
            config = app.config.get('osaconf')
        else:
            config = self.config

        out_dict=mock_query(self.par_dic,session_id,self.job_id,self.scratch_dir)

        self.logger.info('============================================================')
        self.logger.info('')

        print ('query doen with job status-->',job_status)

        if off_line == False:
            print('out', out_dict)
            response= jsonify(out_dict)
        else:
            response= out_dict


        return response



    def run_query(self,off_line=False):

        query_type = self.par_dic['query_type']
        product_type = self.par_dic['product_type']

        #JOBID=PID+RAND
        query_status=self.par_dic['query_status']



        if self.par_dic.has_key('instrumet'):
            self.par_dic.pop('instrumet')
        #prod_dictionary = self.instrument.set_pars_from_from(par_dic)






        self.logger.info('product_type %s' % product_type)
        self.logger.info('query_type %s ' % query_type)
        self.logger.info('instrument %s' % self.instrument_name)
        self.logger.info('parameters dictionary')

        for key in self.par_dic.keys():
            log_str = 'parameters dictionary, key=' + key + ' value=' + str(self.par_dic[key])
            self.logger.info(log_str)

        if self.config is None:
            config = app.config.get('osaconf')
        else:
            config=self.config

        print('conf', config.dispatcher_port)

        job = Job(work_dir=self.scratch_dir,
                  server_url=self.get_current_ip(),
                  server_port=config.dispatcher_port,
                  callback_handle='call_back',
                  session_id=self.par_dic['session_id'],
                  job_id=self.job_id)

        job_monitor=job.monitor

        print ('-----------------> query status  old',query_status )

        if query_status=='new' or query_status=='ready':
            query_out = self.instrument.run_query(product_type,
                                                    self.par_dic,
                                                    request,
                                                    self,
                                                    job,
                                                    out_dir=self.scratch_dir,
                                                    config=config,
                                                    query_type=query_type,
                                                    logger=self.logger)


            print('-----------------> query status job (after query)', job.status)
            if query_out.status_dictionary['status']==0:
                if job.status!='done':
                    job.set_submitted()
                    query_new_status = 'progress'
                else:
                    query_new_status = 'done'
            else:
                query_new_status = 'failed'

            print('-----------------> query status new', query_new_status)

        elif query_status=='progress' or query_status=='unaccessible':

            job_monitor = job.get_dataserver_status(work_dir=self.scratch_dir)
            print('-----------------> query status job (from data server)', job_monitor['status'])
            if job_monitor['status']=='done':
                query_new_status='ready'
            else:
                query_new_status=job_monitor['status']

            print('-----------------> query status new', query_new_status)

            out_dict = {}
            out_dict['job_monitor'] = job_monitor
            out_dict['query_status'] = query_new_status
            out_dict['products'] = ''
            out_dict['exit_status'] = 0
            return out_dict

        elif query_status=='failed':
            #TODO: here we shoudl rusubmit query to get exception from ddosa
            out_dict = {}
            query_new_status='failed'
            out_dict['job_monitor'] = job_monitor
            out_dict['query_status'] = query_new_status
            out_dict['products'] = ''
            out_dict['exit_status'] = -1
            print('-----------------> query status new', query_new_status)
            return out_dict









        self.logger.info('============================================================')
        self.logger.info('')

        out_dict = {}
        out_dict['query_status']=query_new_status
        out_dict['products'] = query_out.prod_dictionary
        out_dict['exit_status'] = query_out.status_dictionary
        print('exit_status', out_dict['exit_status'])

        #if no_job_class_found == False:
        out_dict['job_monitor'] = job_monitor
        #else:
        #    out_dict['job_monitor']= 'not found'
        #    query_out.set_status(1, error_message='job monitor not found in query_out', )

        if off_line == True:
            return out_dict
        else:
            try:
                return jsonify(out_dict)
            except Exception as e:
                query_out.set_status(1,error_message='failed json serialization',debug_message=str(e.message))
                out_dict['exit_status'] = query_out.status_dictionary
                return jsonify(out_dict)


@app.route("/test_sleep")
def test_sleep():
    import time
    time.sleep(10)
    return "<h1 style='color:blue'>Hello There!</h1>"


@app.route("/test_soon")
def test_soon():
    return "<h1 style='color:blue'>Hello There!</h1>"

@app.route('/meta-data')
def meta_data():
    instrument_name = 'ISGRI'
    query = InstrumentQueryBackEnd(instrument_name=instrument_name)
    return query.get_meta_data()


@app.route('/meta-data-src')
def meta_data_src():
    query = InstrumentQueryBackEnd()
    return query.get_meta_data('src_query')
    # return render_template('analysis_display_app.html', form=form,image_html='')


@app.route('/meta-data-instrument')
def meta_data_isgri():
    instrument_name = 'ISGRI'
    query = InstrumentQueryBackEnd(instrument_name=instrument_name)
    return query.get_meta_data('isgri')


@app.route("/download_products",methods=['POST', 'GET'])
def download_products():
    instrument_name = 'ISGRI'
    query = InstrumentQueryBackEnd(instrument_name=instrument_name)
    return query.download_products()


@app.route('/test', methods=['POST', 'GET'])
def run_analysis_test():
    #instrument_name='ISGRI'
    query=InstrumentQueryBackEnd()
    return query.run_query()


@app.route('/test_mock', methods=['POST', 'GET'])
def test_mock():
    #instrument_name='ISGRI'
    query=InstrumentQueryBackEnd()
    return query.run_query_mock()


@app.route('/call_back', methods=['POST', 'GET'])
def dataserver_call_back():
    #instrument_name='ISGRI'
    print('===========================> dataserver_call_back')
    query=InstrumentQueryBackEnd(instrument_name='mock',data_server_call_back=True)
    query.run_call_back()
    print('===========================>\n\n\n')
    return jsonify({})




def run_app(conf,debug=False,threaded=False):
    app.config['osaconf'] = conf
    app.run(host=conf.dispatcher_url, port=conf.dispatcher_port, debug=debug,threaded=threaded)



if __name__ == "__main__":
   app.run()