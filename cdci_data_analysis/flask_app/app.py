#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May 10 10:55:20 2017

@author: andrea tramcere
"""

from __future__ import absolute_import, division, print_function

from builtins import (bytes, open, str, super, range,
                      zip, round, input, int, pow, object, map, zip)

import numpy as np
import os
from flask import jsonify,send_from_directory
from flask import Flask, request
from pathlib import Path
from ..ddosa_interface.osa_isgri import OSA_ISGRI
from ..analysis.queries import *
import  tempfile
import tarfile
import gzip
import logging
import sys



# from ..ddosa_interface.osa_spectrum_dispatcher import  OSA_ISGRI_SPECTRUM
#from ..ddosa_interface.osa_lightcurve_dispatcher import OSA_ISGRI_LIGHTCURVE

from ..web_display import draw_dummy

app = Flask(__name__)

def set_session_logger(scratch_dir):
    logging.basicConfig(filename=os.path.join(scratch_dir,'session.log'),
                        level=logging.DEBUG,
                        filemode='w',
                        format='%(asctime)s %(message)s')

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
    set_session_logger(wd)

    return wd

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



def prepare_download(file_list,file_name):
    if hasattr(file_list,'__iter__'):
        print('file_list is iterable')
    else:
        file_list=[file_list]

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
    print('in url file_list',request.args.get('file_list'))
    file_list=request.args.get('file_list').split(',')
    print('used file_list',file_list)
    file_name=request.args.get('file_name')

    tmp_dir,target_file=prepare_download(file_list,file_name)
    print ('tmp_dir,target_file',tmp_dir,target_file)
    try:
        return send_from_directory(directory=tmp_dir, filename=target_file,attachment_filename=target_file,as_attachment=True)
    except Exception as e:
        return str(e)









@app.route('/test', methods=['POST', 'GET'])
def run_analysis_test():

    instrument_name='ISGRI'


    scratch_dir=set_session(request.args.get('session_id'))

    logger.info('============================================================')
    logger.info('=>session_id<=%s' % request.args.get('session_id'))



    instrument = None
    if instrument_name == 'ISGRI':
        instrument = OSA_ISGRI()

    if instrument is None:
        raise Exception("instrument not recognized".format(instrument_name))


    logger.info(request.args.to_dict())

    prod_dictionary = None
    par_dic = request.args.to_dict()
    par_dic.pop('query_type')
    par_dic.pop('product_type')
    #par_dic.pop('object_name')

    print('par_dic', par_dic)
    print('request', request)


    query_dictionary={}
    query_dictionary['isgri_image']='isgri_image_query'
    query_dictionary['isgri_spectrum'] = 'isgri_spectrum_query'
    query_dictionary['isgri_lc'] = 'isgri_lc_query'

    if request.method == 'GET':


        instrument.set_pars_from_dic(par_dic)
        instrument.show_parameters_list()
        instrument.set_catalog(par_dic,scratch_dir=scratch_dir)

        query_type=request.args.get('query_type')

        product_type=request.args.get('product_type')

        logger.info('product_type %s \n'%product_type)
        logger.info('query_type %s \n' % query_type)
        logger.info('instrument %s\n'%instrument_name)
        logger.info('parameters dictionary \n')
        for key in par_dic.kyes():
            log_str='parameters dictionary, key='+key+' value='+str(par_dic[key])+'\n'
            logger.info(log_str)

        print('product_type',product_type,query_dictionary[product_type])
        prod_dictionary = instrument.run_query(query_dictionary[product_type],
                                               out_dir=scratch_dir,
                                               config=app.config.get('osaconf'),
                                               query_type=query_type)

    logger.info('============================================================')

    return jsonify(prod_dictionary)














def run_app(conf):
    app.config['osaconf'] = conf
    app.run(host=conf.dispatcher_url, port=conf.dispatcher_port, debug=True)

