#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May 10 10:55:20 2017

@author: andrea tramcere
"""

from __future__ import absolute_import, division, print_function

from builtins import (bytes, open,str,super, range,
                      zip, round, input, int, pow, object, map, zip)




from flask import Flask, render_template, request,jsonify


from ..ddosa_interface.osa_isgri import OSA_ISGRI
from ..analysis.products import *
from ..ddosa_interface.osa_spectrum_dispatcher import  OSA_ISGRI_SPECTRUM
from ..ddosa_interface.osa_lightcurve_dispatcher import OSA_ISGRI_LIGHTCURVE


from ..web_display import draw_dummy

app = Flask(__name__)



def get_meta_data(name=None):
    src_query = SourceQuery('src_query')
    isgri = OSA_ISGRI()
    l=[]
    if name is None:
         l.append(src_query.get_parameters_list_as_json())
         l.append(isgri.get_parameters_list_as_json())

    if name=='src_query':
        l=[src_query.get_parameters_list_as_json()]

    if name=='isgri':
        l=[isgri.get_parameters_list_as_json()]

    return jsonify(l)



@app.route('/meta-data')
def meta_data():
    return  get_meta_data()



@app.route('/meta-data-src')
def meta_data_src():
    return  get_meta_data('src_query')
    #return render_template('analysis_display_app.html', form=form,image_html='')


@app.route('/meta-data-isgri')
def meta_data_isgri():


    return get_meta_data('isgri')


@app.route('/test', methods=['POST', 'GET'])
def run_analysis_test():
    #print('osa conf', app.config.get('osaconf'), request.method)

    instrument_name=request.args.get('instrument')
    prod_type=request.args.get('product_type')
    print ('product_type',prod_type)
    print ('instrument', instrument_name)


    instrument=None
    if instrument_name=='osa_isgri':
        instrument=OSA_ISGRI()

    if instrument is None:
        raise Exception("product_type {} not recognized".format(prod_type))


    #sprod.parameters
    if request.method == 'GET':
        print('request', request)
        instrument.set_pars_from_dic(request.args)


        if request.args.get('product_type') == 'isgri_image':

            image, catalog, exception = instrument.get_analysis_product('isgri_image', config=app.config.get('osaconf'))
            html_fig= instrument.get_html_draw(image)

        else:
            # print('osa conf',app.config.get('osaconf'))
            html_fig = draw_dummy()


        #return out_prod
        res = {}
        res['image'] = html_fig
        import numpy as np
        catalog = np.array([(1, 2., 'Hello'), (2, 3., "World")], dtype=[('foo', 'i4'), ('bar', 'f4'), ('baz', 'S10')])
        res['catalog'] = catalog.tolist()
        res['catalog_col_names'] = catalog.dtype.names

        print(html_fig)
        return jsonify(res)

        return jsonify(html_fig)

    return jsonify("invalid method")


def run_app(conf):
    app.config['osaconf'] = conf
    app.run(host=conf.dispatcher_url, port=conf.dispatcher_port, debug=True)

