
from __future__ import absolute_import, division, print_function

from builtins import (open, str, range,
                      object)

from flask import Flask, jsonify, abort
from astropy.table import Table
import json
import  base64
import pickle

micro_service = Flask("micro_service")


@micro_service.route('/api/v1.0/magic', methods=['GET'])
def get_SED():
    #with open("MAGIC_data/19e/magic_19e_sed_fig3_mwl_target01.ecsv") as read_file:
    #    table_text = read_file.read()
    #out_dict={}
    #out_dict['products']={}
    #out_dict['products']['astropy_table_product_list'] = [json.dumps(table_text)]
    # print ( 'ECCO',out_dict['products']['numpy_data_product_list'],_p,_npdl)
    t = Table.read('MAGIC_data/19e/magic_19e_sed_fig3_mwl_target01.ecsv', format='ascii')
    _binarys = base64.b64encode(pickle.dumps(t,protocol=2)).decode('utf-8')
    _o_dict = {}
    _o_dict['products'] = _binarys
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
#    micro_service.run(host="localhost", port=12345)