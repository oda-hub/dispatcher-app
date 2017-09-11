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

from flask import Flask, render_template, request, jsonify
from flask import Session,session,redirect
from flask import make_response
from flask.json import JSONEncoder
import  simplejson
from ..ddosa_interface.osa_isgri import OSA_ISGRI
from ..analysis.queries import *
# from ..ddosa_interface.osa_spectrum_dispatcher import  OSA_ISGRI_SPECTRUM
#from ..ddosa_interface.osa_lightcurve_dispatcher import OSA_ISGRI_LIGHTCURVE

from ..web_display import draw_dummy

app = Flask(__name__)



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


@app.route('/test', methods=['POST', 'GET'])
def run_analysis_test():
    # Set the session
    #s = Session()
    app.secret_key('dhxw\x8f\x05y\x8f\xa2\x13\xb9\xcc\xd6\xc6Z\xc2\x01I\xb3\x91\x9aV\x16\xc2')
    session_id = session.get('session_id')

    # Try to set the cookie
    #if s.setSession():
    resp = redirect('/')
    resp.set_cookie('sessiod_ID', session_id)

    print ('session ID')
    instrument_name='ISGRI'
    prod_type = request.args.get('product_type')
    print('product_type', prod_type)
    print('instrument', instrument_name)

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
        set_catalog(instrument, par_dic)

        if request.args.get('product_type') == 'isgri_image':
            prod_dictionary = query_isgri_image(instrument)


        if request.args.get('product_type') == 'isgri_spectrum':
            prod_dictionary=query_isgri_spectrum(instrument)

        if request.args.get('product_type') == 'isgri_lc':
            prod_dictionary=query_isgri_light_curve(instrument)

    return jsonify(prod_dictionary)



def set_catalog(instrument,par_dic):
    if 'catalog_selected_objects' in par_dic.keys():

        catalog_selected_objects = np.array(par_dic['catalog_selected_objects'].split(','), dtype=np.int)
        #par_dic.pop('catalog_selected_objects')
    else:
        catalog_selected_objects = None

    if catalog_selected_objects is not None:
        from cdci_data_analysis.analysis.catalog import BasicCatalog

        user_catalog = BasicCatalog.from_fits_file('query_catalog.fits')


        print('catalog_length', user_catalog.length)
        instrument.set_par('user_catalog', user_catalog)
        print('catalog_selected_objects', catalog_selected_objects)

        #_sel = np.zeros(user_catalog.length, dtype=bool)
        #_sel[catalog_selected_objects] = True
        #user_catalog.selected = _sel
        user_catalog.select_IDs(catalog_selected_objects)
        print('catalog selected\n',user_catalog.table)
        print('catalog_length', user_catalog.length)




def query_isgri_image(instrument):
    detection_significance = instrument.get_par_by_name('detection_threshold').value



    if request.args.get('query_type') != 'Dummy':

        prod_list, exception = instrument.get_query_products('isgri_image_query', config=app.config.get('osaconf'))


    else:
        prod_list, exception=instrument.get_query_dummy_products('isgri_image_query', config=app.config.get('osaconf'))




    query_image = prod_list.get_prod_by_name('isgri_mosaic')
    query_catalog = prod_list.get_prod_by_name('mosaic_catalog')




    if detection_significance is not None:
        query_catalog.catalog.selected = np.logical_and(query_catalog.catalog._table['significance'] > float(detection_significance),query_catalog.catalog.selected)





    print('--> query was ok')
    query_image.write('query_mosaic.fits', overwrite=True)
    query_catalog.write('query_catalog.fits', overwrite=True)

    html_fig = query_image.get_html_draw(catalog=query_catalog.catalog)
    prod = {}
    prod['image'] = html_fig
    prod['catalog'] = query_catalog.catalog.get_dictionary()

    print ('--> send prog')

    return prod

def query_isgri_spectrum(instrument):
    if request.args.get('query_type') != 'Dummy':
        query_spectra_list, exception = instrument.get_query_products('isgri_spectrum_query', config=app.config.get('osaconf'))
    else:
        query_spectra_list, exception = instrument.get_query_dummy_products('isgri_spectrum_query', config=app.config.get('osaconf'))


    for query_spec in query_spectra_list.prod_list:
        query_spec.write()

    print('--> query was ok')

    prod = {}
    _names=[]
    _figs=[]
    for query_spec in query_spectra_list.prod_list:
        _figs.append( query_spec.get_html_draw(plot=False))
        _names.append(query_spec.name)

    prod['spectrum_name'] = _names
    prod['spectrum_figure']=_figs
    print('--> send prog')
    return prod




def query_isgri_light_curve(instrument):

    if request.args.get('query_type') != 'Dummy':
        prod_list, exception = instrument.get_query_products('isgri_lc_query', config=app.config.get('osaconf'))
    else:
        prod_list, exception = instrument.get_query_dummy_products('isgri_lc_query', config=app.config.get('osaconf'))


    query_lc = prod_list.get_prod_by_name('isgri_lc')
    query_lc.write('query_lc.fits', overwrite=True)


    html_fig = query_lc.get_html_draw()



    prod = {}
    prod['image'] = html_fig

    print ('--> send prog')

    return prod


def run_app(conf):
    app.config['osaconf'] = conf
    app.run(host=conf.dispatcher_url, port=conf.dispatcher_port, debug=True)

