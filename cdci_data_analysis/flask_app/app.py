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


from ..ddosa_interface.osa_image_dispatcher import OSA_ISGRI_IMAGE



from ..web_display import draw_fig

app = Flask(__name__)





@app.route('/')
def index():
    im = OSA_ISGRI_IMAGE()
    im.parameters

    return
    #return render_template('analysis_display_app.html', form=form,image_html='')





@app.route('/test', methods=['POST', 'GET'])
def run_analysis_test():
    print('osa conf', app.config.get('osaconf'), request.method)
    im = app.config.get('Image')

    if request.method == 'GET':
        print('request', request)
        par_names=['E1','E2','T1','T2']

        for p in par_names:
            print('set from form',p,request.args.get(p))
            im.set_par_value(p, request.args.get(p))
            print('--')
        im.set_par_value('time_group_selector','scw_list')
        im.show_parameters_list()
        if request.args.get('image_type') == 'Real':
            image_paht, exception=im.get_product(config=app.config.get('osaconf'))
            image_html = draw_fig(image_paht)
        else:
            # print('osa conf',app.config.get('osaconf'))
            image_html = draw_fig(None, dummy=True)



        return jsonify(image_html)

    return None


def run_app(conf):
    app.config['osaconf'] = conf
    im = OSA_ISGRI_IMAGE()
    im.parameters
    app.config['Image']=im
    app.run(host=conf.dispatcher_url, port=conf.dispatcher_port, debug=True)

