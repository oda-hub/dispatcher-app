from flask import Flask, jsonify, abort
from oda_api.data_products import NumpyDataProduct, NumpyDataUnit
from astropy.table import Table
import json
micro_service = Flask("micro_service")


@micro_service.route('/address_book/api/v1.0/addresses', methods=['GET'])
def get_SED():
    t = Table.read('MAGIC_data/19e/magic_19e_sed_fig3_mwl_target01.ecsv', format='ascii')
    t.write('test.fits', format='fits', overwrite=True)
    p=NumpyDataProduct.from_fits_file('test.fits', meta_data=t.meta, name='SED')
    out_dict={}
    out_dict['products']=None
    out_dict['products']['numpy_data_product_list'] = [p.encode()]
    # print ( 'ECCO',out_dict['products']['numpy_data_product_list'],_p,_npdl)
    return jsonify(out_dict)

def run_micro_service(conf,debug=False,threaded=False):
    micro_service.config['conf'] = conf
    #if conf.sentry_url is not None:
    print('conf micro',micro_service.config['conf'])
    #sentry = Sentry(app, dsn=conf.sentry_url)
    micro_service.run(host=conf.microservice_url, port=conf.microservice_port, debug=debug,threaded=threaded)

#if __name__ == '__main__':
#    micro_service.run(host="localhost", port=12345)