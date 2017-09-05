#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May 10 10:55:20 2017

@author: andrea tramcere
"""

from __future__ import absolute_import, division, print_function

from builtins import (bytes, open, str, super, range,
                      zip, round, input, int, pow, object, map, zip)

from flask import Flask, render_template, request, jsonify
from flask.json import JSONEncoder
import  simplejson
from ..ddosa_interface.osa_isgri import OSA_ISGRI
from ..analysis.queries import *
# from ..ddosa_interface.osa_spectrum_dispatcher import  OSA_ISGRI_SPECTRUM
from ..ddosa_interface.osa_lightcurve_dispatcher import OSA_ISGRI_LIGHTCURVE

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
    # print('osa conf', app.config.get('osaconf'), request.method)
    print(request.args)
    #instrument_name = request.args.get('instrument')
    instrument_name='ISGRI'
    prod_type = request.args.get('product_type')
    print('product_type', prod_type)
    print('instrument', instrument_name)

    instrument = None
    if instrument_name == 'ISGRI':
        instrument = OSA_ISGRI()

    if instrument is None:
        raise Exception("instrument not recognized".format(instrument_name))

    # sprod.parameters
    prod = None
    par_dic = request.args.to_dict()
    par_dic.pop('image_type')
    #par_dic.pop('instrument')
    par_dic.pop('product_type')
    par_dic.pop('object_name')

    print(par_dic)
    if request.method == 'GET':

        if request.args.get('product_type') == 'isgri_image':
            prod_dictionary = query_isgri_image(instrument, par_dic)

    return jsonify(prod_dictionary)


def query_isgri_image(instrument, par_dic):
    print('request.files', request.files)
    if 'catalog_selected_objects' in par_dic.keys():
        import numpy as np
        catalog_selected_objects = np.array(par_dic['catalog_selected_objects'].split(','), dtype=np.int)
        par_dic.pop('catalog_selected_objects')
    else:
        catalog_selected_objects = None

    print('request', request)

    print('par_dic', par_dic)
    instrument.set_pars_from_dic(par_dic)

    instrument.show_parameters_list()
    if request.args.get('image_type') != 'Dummy':

        if catalog_selected_objects is not None:
            # if instrument.get_par_by_name('user_catalog').value is None:
            #    instrument.set_par('user_catalog', 'query_catalog.fits')

            from cdci_data_analysis.analysis.catalog import BasicCatalog

            user_catalog = BasicCatalog.from_fits_file('query_catalog.fits')
            print('catalog_length', user_catalog.length)
            instrument.set_par('user_catalog', user_catalog)
            print('catalog_selected_objects', catalog_selected_objects)

            _sel = np.zeros(user_catalog.length, dtype=bool)
            _sel[catalog_selected_objects] = True
            user_catalog.selected = _sel
            print('catalog selected',user_catalog.table)
            print('catalog_length', user_catalog.length)

        prod_list, exception = instrument.get_query_products('isgri_image_query', config=app.config.get('osaconf'))

        query_image = prod_list.get_prod_by_name('isgri_mosaic')
        query_catalog = prod_list.get_prod_by_name('mosaic_catalog')

        detection_significance = instrument.get_par_by_name('detection_threshold').value

        if detection_significance is not None:
            query_catalog.catalog.selected = query_catalog.catalog.table['significance'] > float(detection_significance)

        html_fig = query_image.get_html_draw(catalog=query_catalog.catalog)

    else:
        # print('osa conf',app.config.get('osaconf'))
        html_fig = draw_dummy()

        return jsonify(html_fig)

    print ('--> query was ok')
    prod = {}
    prod['image'] = html_fig
    prod['catalog'] = query_catalog.catalog.get_dictionary()

    query_image.write('query_mosaic.fits', overwrite=True)
    query_catalog.write('query_catalog.fits', overwrite=True)
    
    print ('--> send prog')
    return prod

def query_isgri_spectrum():
    pass


def query_isgri_ligthcurve():
    pass


def run_app(conf):
    app.config['osaconf'] = conf
    app.run(host=conf.dispatcher_url, port=conf.dispatcher_port, debug=True)

