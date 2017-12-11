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
from flask import jsonify,send_from_directory
from flask import Flask, request
from flask import render_template
from flask.views import View

#from pathlib import Path
#from flask_restful import reqparse

from ..ddosa_interface.osa_isgri import OSA_ISGRI
from ..ddosa_interface.osa_jemx import OSA_JEMX
from ..analysis.queries import *
import  tempfile
import tarfile
import gzip
import logging
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

    def __init__(self,instrument_name=None,par_dic=None,config=None):
        #self.instrument_name=instrument_name

        if par_dic is None:
            self.set_args(request)
        else:
            self.par_dic = par_dic

        if instrument_name is None:
            instrument_name = self.par_dic['instrument']


        self.set_scratch_dir(self.par_dic['session_id'])
        self.set_session_logger(self.scratch_dir)
        self.set_instrument(instrument_name)
        self.config=config

    def set_instrument(self,instrument_name):
        if instrument_name == 'ISGRI':
            self.instrument = OSA_ISGRI()
        elif instrument_name=='JEMX':
            self.instrument=OSA_JEMX()

        if self.instrument is None:
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

    def set_args(self,request):
        if request.method == 'GET':
            args = request.args
        if request.method == 'POST':
            args = request.form
        self.par_dic = args.to_dict()
        print('par_dic', self.par_dic)

        self.args=args

    def set_scratch_dir(self,session_id):
        wd = './scratch'
        if session_id is not None:
            wd = 'scratch_' + session_id

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


    def run_query(self,off_line=False):

        query_type = self.par_dic['query_type']
        product_type = self.par_dic['product_type']

        self.par_dic.pop('query_type')
        self.par_dic.pop('product_type')
        if self.par_dic.has_key('instrumet'):
            self.par_dic.pop('instrumet')
        #prod_dictionary = self.instrument.set_pars_from_from(par_dic)

        #if prod_dictionary['status'] == 0:





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

        query_out = self.instrument.run_query(product_type,
                                                self.par_dic,
                                                request,
                                                self,
                                                out_dir=self.scratch_dir,
                                                config=config,
                                                query_type=query_type,
                                                logger=self.logger)


        self.logger.info('============================================================')
        self.logger.info('')

        if off_line==False:
            try:
                out_dict={}
                out_dict['products']=query_out.prod_dictionary
                out_dict['exit_status'] = query_out.status_dictionary
                print('exit_status',out_dict['exit_status'])
                return jsonify(out_dict)
            except Exception as e:
                query_out.set_status(1,error_message='failied json serialization',debug_message=str(e.message))
                out_dict['exit_status'] = query_out.status_dictionary
                return jsonify(out_dict)

        else:
            return query_out




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






def run_app(conf,debug=False,threaded=False):
    app.config['osaconf'] = conf
    app.run(host=conf.dispatcher_url, port=conf.dispatcher_port, debug=debug,threaded=threaded)



if __name__ == "__main__":
   app.run()