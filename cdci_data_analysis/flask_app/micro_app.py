
from __future__ import absolute_import, division, print_function

from builtins import (open, str, range,
                      object)

from flask import Flask, jsonify, abort,request
from oda_api.data_products import AstropyTable
from astropy.table import Table
from flask.json import JSONEncoder
import  numpy as np
import json
import yaml

micro_service = Flask("micro_service")

class CustomJSONEncoder(JSONEncoder):

    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return list(obj)
        return JSONEncoder.default(self, obj)


app = Flask(__name__)
app.json_encoder = CustomJSONEncoder

@micro_service.route('/api/v1.0/magic/get-catalog', methods=['GET','POST'])
def get_catalog():

    with open('MAGIC_data/19e/magic_19e.yaml') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)

    _o_dict = json.dumps(data)
    return jsonify(_o_dict)


@micro_service.route('/api/v1.0/magic/get-data', methods=['GET','POST'])
def get_data():
    if request.method == 'GET':
        args = request.args
    if request.method == 'POST':
        args = request.form

    par_dic = args.to_dict()
    file_name=par_dic['file_name']

    #with open("MAGIC_data/19e/magic_19e_sed_fig3_mwl_target01.ecsv") as read_file:
    #    table_text = read_file.read()
    #out_dict={}
    #out_dict['products']={}
    #out_dict['products']['astropy_table_product_list'] = [json.dumps(table_text)]
    # print ( 'ECCO',out_dict['products']['numpy_data_product_list'],_p,_npdl)
    t = AstropyTable(Table.read('MAGIC_data/19e/%s.ecsv'%file_name, format='ascii'),name='MAGIC TABLE')

    _o_dict = {}
    _o_dict['astropy_table'] = t.encode(use_binary=False)
    _o_dict = json.dumps(_o_dict)
    #_o_dict = json.loads(_o_dict)
    #t_rec = base64.b64decode(_o_dict['table'])
    #t_rec = pickle.loads(t_rec)
    #t_rec

    return jsonify(_o_dict)

def run_micro_service(conf,debug=False,threaded=False):
    micro_service.config['conf'] = conf
    #if conf.sentry_url is not None:
    print('conf micro',micro_service.config['conf'])
    #sentry = Sentry(app, dsn=conf.sentry_url)
    micro_service.run(host=conf.microservice_url, port=conf.microservice_port, debug=debug,threaded=threaded)

#if __name__ == '__main__':
#    micro_service.run(host="0.0.0.0", port=32790)