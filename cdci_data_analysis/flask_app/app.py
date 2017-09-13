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
from flask import Flask, render_template, request, jsonify,send_from_directory
from flask import Flask, session, redirect, url_for, escape, request
from flask import make_response
from flask.json import JSONEncoder
import  simplejson
from pathlib import Path
from ..ddosa_interface.osa_isgri import OSA_ISGRI
from ..analysis.queries import *
import  tempfile
import tarfile
from uuid import uuid4

# from ..ddosa_interface.osa_spectrum_dispatcher import  OSA_ISGRI_SPECTRUM
#from ..ddosa_interface.osa_lightcurve_dispatcher import OSA_ISGRI_LIGHTCURVE

from ..web_display import draw_dummy

app = Flask(__name__)


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
    wd='./'
    if session_id is not None:
        wd = 'scratch_'+session_id
        make_dir(wd)

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



def make_tar(spec_file,arf_file,rmf_file):
    tmp_dir=tempfile.mkdtemp('download')
    print ('using tmp dir',tmp_dir)
    make_dir(tmp_dir)

    tar = tarfile.open("%s/spectra.tar"%tmp_dir, "w")
    for name in [spec_file,arf_file,rmf_file]:
        print ('add to tar',name)
        if name is not None:
            tar.add(name)
    tar.close()
    return tmp_dir,'spectra.tar'

@app.route("/download_spectra",methods=['POST', 'GET'])
def download_spectra ():
    spec_file=request.args.get('spec_file')
    arf_file=request.args.get('arf_file')
    rmf_file = request.args.get('rmf_file')
    print('download spec file',spec_file)
    print('download arf file', arf_file)
    print('download rmf file', rmf_file)
    tmp_dir,tar_file=make_tar(spec_file,arf_file,rmf_file)
    print ('tmp_dir,tar_file',tmp_dir,tar_file)
    try:
        return send_from_directory(directory=tmp_dir, filename=tar_file)
    except Exception as e:
        return str(e)


@app.route('/test', methods=['POST', 'GET'])
def run_analysis_test():
    #print (session)
    #session_id=str(uuid4())

    # Try to set the cookie
    #if s.setSession():

    #if request.cookies.get('sessiod_ID') is None:

    #   resp = make_response(redirect('/test'))
    #    print ('write session ID')
    #    resp.set_cookie('sessiod_ID', session_id)

    #print ('session ID',session_id)
    instrument_name='ISGRI'
    prod_type = request.args.get('product_type')
    print('product_type', prod_type)
    print('instrument', instrument_name)


    print('=>session_id<='),request.args.get('session_id')

    scratch_dir=set_session(request.args.get('session_id'))

    instrument = None
    if instrument_name == 'ISGRI':
        instrument = OSA_ISGRI()

    if instrument is None:
        raise Exception("instrument not recognized".format(instrument_name))

    prod_dictionary = None
    par_dic = request.args.to_dict()
    par_dic.pop('query_type')

    par_dic.pop('product_type')
    #par_dic.pop('object_name')

    print('par_dic', par_dic)
    print('request', request)

    if request.method == 'GET':


        instrument.set_pars_from_dic(par_dic)
        instrument.show_parameters_list()
        set_catalog(instrument, par_dic,scratch_dir=scratch_dir)

        if request.args.get('product_type') == 'isgri_image':
            prod_dictionary = query_isgri_image(instrument,scratch_dir=scratch_dir)


        if request.args.get('product_type') == 'isgri_spectrum':
            prod_dictionary=query_isgri_spectrum(instrument,scratch_dir=scratch_dir)

        if request.args.get('product_type') == 'isgri_lc':
            prod_dictionary=query_isgri_light_curve(instrument,scratch_dir=scratch_dir)

    return jsonify(prod_dictionary)



def set_catalog(instrument,par_dic,scratch_dir='./'):
    if 'catalog_selected_objects' in par_dic.keys():

        catalog_selected_objects = np.array(par_dic['catalog_selected_objects'].split(','), dtype=np.int)
        #par_dic.pop('catalog_selected_objects')
    else:
        catalog_selected_objects = None

    if catalog_selected_objects is not None:
        from cdci_data_analysis.analysis.catalog import BasicCatalog

        file_path=Path(scratch_dir,'query_catalog.fits')
        print('using catalog',file_path)
        user_catalog = BasicCatalog.from_fits_file(file_path)


        print('catalog_length', user_catalog.length)
        instrument.set_par('user_catalog', user_catalog)
        print('catalog_selected_objects', catalog_selected_objects)

        #_sel = np.zeros(user_catalog.length, dtype=bool)
        #_sel[catalog_selected_objects] = True
        #user_catalog.selected = _sel
        user_catalog.select_IDs(catalog_selected_objects)
        print('catalog selected\n',user_catalog.table)
        print('catalog_length', user_catalog.length)




def query_isgri_image(instrument,scratch_dir='./'):
    detection_significance = instrument.get_par_by_name('detection_threshold').value



    if request.args.get('query_type') != 'Dummy':

        prod_list, exception = instrument.get_query_products('isgri_image_query', config=app.config.get('osaconf'),out_dir=scratch_dir)


    else:
        prod_list, exception=instrument.get_query_dummy_products('isgri_image_query', config=app.config.get('osaconf'),out_dir=scratch_dir)




    query_image = prod_list.get_prod_by_name('isgri_mosaic')
    query_catalog = prod_list.get_prod_by_name('mosaic_catalog')




    if detection_significance is not None:
        query_catalog.catalog.selected = np.logical_and(query_catalog.catalog._table['significance'] > float(detection_significance),query_catalog.catalog.selected)





    print('--> query was ok')
    #file_path = Path(scratch_dir, 'query_mosaic.fits')
    query_image.write(overwrite=True)
    #file_path = Path(scratch_dir, 'query_catalog.fits')
    query_catalog.write(overwrite=True)

    html_fig = query_image.get_html_draw(catalog=query_catalog.catalog)
    prod = {}
    prod['image'] = html_fig
    prod['catalog'] = query_catalog.catalog.get_dictionary()

    print ('--> send prog')

    return prod

def query_isgri_spectrum(instrument,scratch_dir='./'):
    if request.args.get('query_type') != 'Dummy':
        query_spectra_list, exception = instrument.get_query_products('isgri_spectrum_query', config=app.config.get('osaconf'),out_dir=scratch_dir)
    else:
        query_spectra_list, exception = instrument.get_query_dummy_products('isgri_spectrum_query', config=app.config.get('osaconf'),out_dir=scratch_dir)


    for query_spec in query_spectra_list.prod_list:
        query_spec.write()

    print('--> query was ok')

    prod = {}
    _names=[]
    _figs=[]
    _spec_path=[]
    for query_spec in query_spectra_list.prod_list:
        _figs.append( query_spec.get_html_draw(plot=False))
        _names.append(query_spec.name)
        d_spec={}
        d_spec['sepc_file']=query_spec.file_path.get_file_path()
        d_spec['arf_file']=query_spec.arf_file.encode('utf-8')
        d_spec['rmf_file']=query_spec.rmf_file.encode('utf-8')
        _spec_path.append(d_spec)


    prod['spectrum_name'] = _names
    prod['spectrum_figure']=_figs
    prod['spectra_path']=_spec_path
    print('--> send prog')
    return prod




def query_isgri_light_curve(instrument,scratch_dir='./'):

    if request.args.get('query_type') != 'Dummy':
        prod_list, exception = instrument.get_query_products('isgri_lc_query', config=app.config.get('osaconf'),out_dir=scratch_dir)
    else:
        prod_list, exception = instrument.get_query_dummy_products('isgri_lc_query', config=app.config.get('osaconf'),out_dir=scratch_dir)


    query_lc = prod_list.get_prod_by_name('isgri_lc')
    query_lc.write(overwrite=True)


    html_fig = query_lc.get_html_draw()



    prod = {}
    prod['image'] = html_fig

    print ('--> send prog')

    return prod


def run_app(conf):
    app.config['osaconf'] = conf
    app.run(host=conf.dispatcher_url, port=conf.dispatcher_port, debug=True)

