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
from pathlib import Path
from flask_restful import reqparse

from ..ddosa_interface.osa_isgri import OSA_ISGRI
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

UPLOAD_FOLDER = '/path/to/the/uploads'
ALLOWED_EXTENSIONS = set(['txt', 'fits', 'fits.gz'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/upload_catalog', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            return redirect(url_for('uploaded_file',
                                    filename=filename))
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <p><input type=file name=file>
         <input type=submit value=Upload>
    </form>
    '''

def set_session_logger(scratch_dir):
    logger = logging.getLogger(__name__)
    fileh = logging.FileHandler(os.path.join(scratch_dir,'session.log'), 'a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileh.setFormatter(formatter)

    log = logging.getLogger()  # root logger
    for hdlr in log.handlers[:]:  # remove all old handlers
        log.removeHandler(hdlr)
    log.addHandler(fileh)  # set the new handler
    logger.setLevel(logging.INFO)
    print ('logfile set to dir=',scratch_dir,' with name=session.log')


    return logger

def make_dir(out_dir):


    if os.path.isdir(out_dir):
        return
    else:
        if os.path.isfile(out_dir):
            raise RuntimeError("a file with the same name of dir already exists")
            #raise RuntimeError, "a file with the same name of dir=%s, exists"%out_dir
        else:
            os.mkdir(out_dir)


def set_session(session_id):
    wd='./scratch'
    if session_id is not None:
        wd = 'scratch_'+session_id

    make_dir(wd)
    logger=set_session_logger(wd)

    return wd,logger

def get_meta_data(name=None):
    src_query = SourceQuery('src_query')
    isgri = OSA_ISGRI()
    l = []
    if name is None:
        l.append(src_query.get_parameters_list_as_json())
        l.append(isgri.get_parameters_list_as_json())

    if name == 'src_query':
        l = [src_query.get_parameters_list_as_json()]

    if name == 'isgri':
        l = [isgri.get_parameters_list_as_json()]

    return jsonify(l)


@app.route('/meta-data')
def meta_data():
    return get_meta_data()


@app.route('/meta-data-src')
def meta_data_src():
    return get_meta_data('src_query')
    # return render_template('analysis_display_app.html', form=form,image_html='')


@app.route('/meta-data-isgri')
def meta_data_isgri():
    return get_meta_data('isgri')

@app.route("/test_sleep")
def test_sleep():

    import time
    time.sleep(10)
    return "<h1 style='color:blue'>Hello There!</h1>"


@app.route("/test_soon")
def test_soon():
    return "<h1 style='color:blue'>Hello There!</h1>"

def prepare_download(file_list,file_name,scratch_dir):
    if hasattr(file_list,'__iter__'):
        print('file_list is iterable')
    else:
        file_list=[file_list]

    for ID,f in enumerate(file_list):
        file_list[ID]=os.path.join(scratch_dir+'/',f)

    tmp_dir=tempfile.mkdtemp(prefix='download_', dir='./')
    print ('using tmp dir',tmp_dir)

    file_path=os.path.join(tmp_dir,file_name)
    print('writing to file path', file_path)

    if len(file_list)>1:
        print ('preparing tar')
        tar = tarfile.open("%s"%(file_path), "w:gz")
        for name in file_list:
            print ('add to tar',name)
            if name is not None:
                tar.add(name)
        tar.close()
    else:
        print('single fits file')
        in_data = open(file_list[0], "rb").read()
        with gzip.open(file_path, 'wb') as f:
            f.write(in_data)

    tmp_dir = os.path.abspath(tmp_dir)

    return tmp_dir,file_name



@app.route("/download_products",methods=['POST', 'GET'])
def download_products():
    args = get_args(request)

    print('in url file_list',args.get('file_list'))
    scratch_dir, logger = set_session(args.get('session_id'))

    file_list=args.get('file_list').split(',')
    print('used file_list',file_list)
    file_name=args.get('file_name')

    tmp_dir,target_file=prepare_download(file_list,file_name,scratch_dir)
    print ('tmp_dir,target_file',tmp_dir,target_file)
    try:
        return send_from_directory(directory=tmp_dir, filename=target_file,attachment_filename=target_file,as_attachment=True)
    except Exception as e:
        return str(e)







#def get_args(arg, **kwargs):
#    parse = reqparse.RequestParser()
#    parse.add_argument(arg, **kwargs)
#    args = parse.parse_args()
#    return args

def upload_file(name,scratch_dir):
    print(request.files)
    if name not in request.files:
        return
    else:
        file = request.files[name]
        print ( 'type file',type(file))
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '' or file.filename is None:
            return None


        filename = secure_filename(file.filename)
        print('secure_file_name',filename)
        file_path=os.path.join(scratch_dir, filename)
        file.save(file_path)
        #return redirect(url_for('uploaded_file',
        #                        filename=filename))
        return file_path

def get_args(request):
    if request.method == 'GET':
        args=request.args
    if request.method == 'POST':
        args = request.form
    print ('args',args)
    return args

@app.route('/test', methods=['POST', 'GET'])
def run_analysis_test():

    instrument_name='ISGRI'

    args=get_args(request)
    print ('method',request.method)
    print ('args',args)


    scratch_dir,logger=set_session(args.get('session_id'))

    logger.info('')
    logger.info('============================================================')
    logger.info('=>session_id<=%s' % args.get('session_id'))



    instrument = None
    if instrument_name == 'ISGRI':
        instrument = OSA_ISGRI()

    if instrument is None:
        raise Exception("instrument not recognized".format(instrument_name))


    logger.info(args.to_dict())

    prod_dictionary = None


    par_dic = args.to_dict()
    par_dic.pop('query_type')
    par_dic.pop('product_type')
    #par_dic.pop('object_name')

    print('par_dic', par_dic)
    #print('request', request)


    query_dictionary={}
    query_dictionary['isgri_image']='isgri_image_query'
    query_dictionary['isgri_spectrum'] = 'isgri_spectrum_query'
    query_dictionary['isgri_lc'] = 'isgri_lc_query'
    query_dictionary['spectral_fit'] = 'spectral_fit_query'

    #if request.method == 'GET':


    instrument.set_pars_from_dic(par_dic)
    instrument.show_parameters_list()



    query_type=args.get('query_type')

    product_type=args.get('product_type')

    logger.info('product_type %s'%product_type)
    logger.info('query_type %s ' % query_type)
    logger.info('instrument %s'%instrument_name)
    logger.info('parameters dictionary')

    for key in par_dic.keys():
        log_str='parameters dictionary, key='+key+' value='+str(par_dic[key])
        logger.info(log_str)

    print('product_type',product_type,query_dictionary[product_type])

    #move the catalog setting to some preprocessing stage
    if request.method == 'POST':
        prod_dictionary={}
        try:
            cat_file_path = upload_file('user_catalog', scratch_dir)
            par_dic['user_catalog'] = cat_file_path
        except Exception as e:
            prod_dictionary['error_message'] = 'failed to upload catalog'
            prod_dictionary['status'] = '1'
            logger.exception(e.message)

    try:
        instrument.set_catalog(par_dic, scratch_dir=scratch_dir)
    except Exception as e:
        prod_dictionary['error_message'] = 'catalog file is not valid'
        prod_dictionary['status'] = '1'
        print(e.message)
        logger.exception(e.message)



    if prod_dictionary=={}:
        prod_dictionary = instrument.run_query(query_dictionary[product_type],
                                               out_dir=scratch_dir,
                                               config=app.config.get('osaconf'),
                                               query_type=query_type,
                                               logger=logger)

    logger.info('============================================================')
    logger.info('')

    return jsonify(prod_dictionary)




def run_app_threaded(conf,debug=False):
    threaded_server=threading.Thread(target=run_app,args=(conf),kwargs={'debug':debug})
    try:
        # Start the server
        threaded_server.start()
    except Exception as ex:
        print('flask thread failed',ex.message)
    finally:

        # Stop all running threads
        threaded_server._Thread__stop()
        product_dictionary={}
        product_dictionary['error_message'] = 'flask thread failed'
        product_dictionary['status'] = '-1'

        return jsonify(product_dictionary)




def run_app(conf,debug=False,threaded=False):
    app.config['osaconf'] = conf
    app.run(host=conf.dispatcher_url, port=conf.dispatcher_port, debug=debug,threaded=threaded)


if __name__ == "__main__":
    run_app()