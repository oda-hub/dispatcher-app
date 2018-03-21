from __future__ import print_function
from builtins import (str, open, range)


import json
import  logging

logger = logging.getLogger(__name__)

from cdci_data_analysis.configurer import ConfigEnv
osaconf = ConfigEnv.from_conf_file('./conf_env_test.yml')

import time
from flask import Flask, request
import flask

from cdci_data_analysis.flask_app.app import InstrumentQueryBackEnd










def build_user_catalog(RA_user_cat,Dec_user_cat):
    cat_dict = {}
    cat_dict['cat_column_list'] = [RA_user_cat, Dec_user_cat, ['TEST_SOURCE'], [10.]]
    cat_dict['cat_column_names'] = ['ra', 'dec', 'src_names', 'significance']
    cat_dict['cat_frame'] = 'fk5'
    cat_dict['cat_coord_units'] = 'deg'
    cat_dict['cat_lon_name'] = 'ra'
    cat_dict['cat_lat_name'] = 'dec'


    return cat_dict

def build_upload_data(data_type):

    if data_type=='cat_fits':
        data = dict(
            user_catalog_file=(open("dummy_prods/query_catalog.fits", "rb", buffering=0), "query_user_catalog"),
        )

    elif data_type=='cat_csv':
        data = dict(
            user_catalog_file=(open("dummy_prods/query_catalog.txt", "rb", buffering=0), "query_user_catalog"),
        )
    elif data_type=='scw_list':
        data = dict(
            user_scw_list_file=(open("dummy_prods/query_scw_list.txt", "rb", buffering=0), "query_scw_list"),
        )

    else:
        raise  RuntimeError("data_type allowed for build_upload_data=cat_fits,cat_csv,scw_list, used",data_type)


    return data


def set_mosaic_query(instrument_name,
                     scw_list=None,
                     user_catalog=False,
                     query_type='Real',
                     upload_data=None,
                     T1_iso='2003-03-15T23:27:40.0',
                     T2_iso='2003-03-16T00:03:15.0',
                     RA_user_cat=[205.09872436523438],
                     Dec_user_cat=[83.6317138671875],
                     session_id='test',
                     detection_threshold=5.0,
                     radius=25,
                     E1_keV=20.,
                     E2_keV=40.):


    #testapp = flask.Flask(__name__)

    #if scw_list==None:
    #    scw_list=cookbook_scw_list

    if user_catalog == True:
        cat_dict=build_user_catalog(RA_user_cat,Dec_user_cat)
    else:
        cat_dict=None

    if instrument_name=='isgri':
        product_type='isgri_image'
    elif instrument_name=='jemx':
        product_type='jemx_image'
    else:
        raise RuntimeError('instrumet %s'%instrument_name, 'not supported')

    parameters_dic=dict(E1_keV=E1_keV,E2_keV=E2_keV,T1=T1_iso, T2=T2_iso,RA=RA_user_cat[0],DEC=Dec_user_cat[0],radius=radius,scw_list=scw_list,
                        image_scale_min=1,session_id=session_id,query_type=query_type,product_type=product_type,
                        detection_threshold=detection_threshold,user_catalog_dictionary=cat_dict)

    if upload_data is not None:
        data=build_upload_data(upload_data)
    else:
        data=None

    return parameters_dic,data





def set_spectrum_query(instrument_name,
                     scw_list=None,
                     user_catalog=False,
                     query_type='Real',
                     upload_data=None,
                     T1_iso='2003-03-15T23:27:40.0',
                     T2_iso='2003-03-16T00:03:15.0',
                     RA_user_cat=[205.09872436523438],
                     Dec_user_cat=[83.6317138671875],
                     session_id='test',
                     detection_threshold=5.0,
                     radius=25,
                     E1_keV=20.,
                     E2_keV=40.):


    if instrument_name == 'isgri':
        product_type = 'isgri_spectrum'
    elif instrument_name == 'jemx':
        product_type = 'jemx_spectrum'
    else:
        raise RuntimeError('instrumet %s' % instrument_name, 'not supported')

    #if scw_list==None:
    #    scw_list=cookbook_scw_list

    if user_catalog == True:
        cat_dict=build_user_catalog(RA_user_cat,Dec_user_cat)
    else:
        cat_dict=None



    parameters_dic=dict(E1_keV=E1_keV,E2_keV=E2_keV,T1=T1_iso, T2=T2_iso,RA=RA_user_cat[0],DEC=RA_user_cat[0],radius=radius,scw_list=scw_list,
                        image_scale_min=1,session_id=session_id,query_type=query_type,product_type=product_type,
                        detection_threshold=detection_threshold,user_catalog_dictionary=cat_dict)

    if upload_data is not None:
        data=build_upload_data(upload_data)
    else:
        data=None

    return parameters_dic,data


def set_spectral_fit_query(instrument_name,
                     job_id,
                     session_id,
                     src_name='4U 1700-377',
                     user_catalog=False,
                     scw_list=None,
                     query_type='Real',
                     upload_data=None,
                     T1_iso='2003-03-15T23:27:40.0',
                     T2_iso='2003-03-16T00:03:15.0',
                     RA_user_cat=[205.09872436523438],
                     Dec_user_cat=[83.6317138671875],
                     detection_threshold=5.0,
                     radius=25,
                     E1_keV=20.,
                     E2_keV=40.):

    if instrument_name == 'isgri':
        product_type = 'spectral_fit'
    elif instrument_name == 'jemx':
        product_type = 'spectral_fit'
    else:
        raise RuntimeError('instrumet %s' % instrument_name, 'not supported')

    #if scw_list == None:
    #    scw_list = cookbook_scw_list

    if user_catalog == True:
        cat_dict = build_user_catalog(RA_user_cat, Dec_user_cat)
    else:
        cat_dict = None

    parameters_dic = dict(E1_keV=E1_keV, E2_keV=E2_keV, T1=T1_iso, T2=T2_iso, RA=RA_user_cat[0], DEC=Dec_user_cat[0],
                          radius=radius, scw_list=scw_list,src_name=src_name,job_id=job_id,query_status='ready',
                          image_scale_min=1, session_id=session_id, query_type=query_type, product_type=product_type,
                          detection_threshold=detection_threshold, user_catalog_dictionary=cat_dict)

    parameters_dic['xspec_model']='powerlaw'
    parameters_dic['ph_file_name'] = 'query_spectrum_isgri_sum_1E_1740.7-2942.fits'
    parameters_dic['rmf_file_name'] = 'query_spectrum_rmf_sum_1E_1740.7-2942.fits.gz'
    parameters_dic['arf_file_name'] = 'query_spectrum_arf_sum_1E_1740.7-2942.fits.gz'


    if upload_data is not None:
        data=build_upload_data(upload_data)
    else:
        data=None

    return parameters_dic, data

def set_lc_query(instrument_name,
                     src_name='4U 1700-377',
                     time_bin=500,
                     user_catalog=False,
                     time_bin_format='sec',
                     scw_list=None,
                     query_type='Real',
                     upload_data=None,
                     T1_iso='2003-03-15T23:27:40.0',
                     T2_iso='2003-03-16T00:03:15.0',
                     RA_user_cat=[205.09872436523438],
                     Dec_user_cat=[83.6317138671875],
                     session_id='test',
                     detection_threshold=5.0,
                     radius=25,
                     E1_keV=20.,
                     E2_keV=40.):


    if instrument_name == 'isgri':
        product_type = 'isgri_lc'
    elif instrument_name == 'jemx':
        product_type = 'jemx_lc'
    else:
        raise RuntimeError('instrumet %s' % instrument_name, 'not supported')

    #if scw_list==None:
    #    scw_list=cookbook_scw_list

    if user_catalog == True:
        cat_dict=build_user_catalog(RA_user_cat,Dec_user_cat)
    else:
        cat_dict=None



    parameters_dic=dict(E1_keV=E1_keV,E2_keV=E2_keV,T1=T1_iso, T2=T2_iso,RA=RA_user_cat[0],DEC=Dec_user_cat[0],radius=radius,scw_list=scw_list,
                        image_scale_min=1,session_id=session_id,query_type=query_type,product_type=product_type,
                        detection_threshold=detection_threshold,src_name=src_name,time_bin=time_bin,time_bin_format=time_bin_format,user_catalog_dictionary=cat_dict)

    if upload_data is not None:
        data=build_upload_data(upload_data)
    else:
        data=None

    return parameters_dic,data






def test_synch_request(parameters_dic,instrument_name,query_status='new',job_id=None,upload_data=None):
    testapp = flask.Flask(__name__)

    parameters_dic['query_status'] = query_status
    parameters_dic['session_id'] = 'asynch_session'
    parameters_dic['job_id'] = job_id

    with testapp.test_request_context(method='POST', content_type='multipart/form-data', data=upload_data):
        query = InstrumentQueryBackEnd(instrument_name=instrument_name, par_dic=parameters_dic, config=osaconf)

        print('request', request.method)
        query_out = query.run_query(off_line=True)

        print('\n\n\n')

        print('query_out:job_monitor', query_out)


def test_asynch_request(parameters_dic,instrument_name,query_status,job_id=None,upload_data=None):
    testapp = flask.Flask(__name__)

    parameters_dic['query_status'] = query_status
    parameters_dic['session_id'] = 'asynch_session'
    parameters_dic['job_id']=job_id


    with testapp.test_request_context(method='POST', content_type='multipart/form-data', data=upload_data):
        query = InstrumentQueryBackEnd(instrument_name=instrument_name, par_dic=parameters_dic, config=osaconf)

        print('\n')
        query_out = query.run_query(off_line=True)

        print('\n\n\n')

        print('query_out', query_out['query_status'])

    return query_out


def test_spectral_fit_query():
    instrument_name = 'isgri'
    parameters_dic, upload_data = set_spectral_fit_query(instrument_name=instrument_name,
                                                         session_id='asynch_session',
                                                         job_id='943QQPH6WUDS8SL1',
                                                         scw_list=asynch_scw_list,
                                                         RA_user_cat=[80.63168334960938],
                                                         Dec_user_cat=[20.01494598388672],
                                                         user_catalog=False, upload_data=None,
                                                         query_type='True')

    testapp = flask.Flask(__name__)
    with testapp.test_request_context(method='POST', content_type='multipart/form-data', data=upload_data):

        query = InstrumentQueryBackEnd(instrument_name=instrument_name, par_dic=parameters_dic, config=osaconf)

        print('\n')
        query_out = query.run_query(off_line=True)

        print('\n\n\n')

        #print('query_out:job_monitor', query_out['job_monitor'])

    print('exit_status', query_out['exit_status'])
    print('job_monitor', query_out['job_monitor'])
    print('query_status', query_out['query_status'])
    print('products', query_out['products'].keys())
    for k in query_out['products'].keys():
        if k == 'image':
            print(k, '=>', query_out['products'][k].keys())
        else:
            print(k, '=>', query_out['products'][k])
    return query_out

crab_scw_list=["035200230010.001","035200240010.001"]
cookbook_scw_list=['005100410010.001','005100420010.001','005100430010.001','005100440010.001','005100450010.001']
asynch_scw_list=['004000030030.001']
asynch_scw_list=['035200230010.001']
asynch_scw_list_jemx=["138700520010.001"]

def test_asynch_full():
    """
    Do not use with set_spectral_fit_query
    :return:
    """

    instrument_name='isgri'
    parameters_dic,upload_data=set_mosaic_query(instrument_name=instrument_name,
                                                T1_iso='2003-03-15T23:27:40.0',
                                                T2_iso='2003-03-16T00:03:15.0',
                                                scw_list=asynch_scw_list,
                                                E1_keV=30.0,
                                                RA_user_cat=[257.815417],
                                                Dec_user_cat=[-41.593417],
                                                user_catalog=False,
                                                #upload_data='cat_csv',
                                                query_type='Dummy')

    print('upload_data', upload_data)
    query_out=test_asynch_request(parameters_dic,instrument_name,query_status='new',upload_data=None)
    query_status=query_out['query_status']
    job_id=query_out['job_monitor']['job_id']
    if query_status!='failed':
        pass
    else:
        failure_report(query_out)
        raise Exception('query failed')


    while query_status!='done' and query_status!='failed':
        query_out = test_asynch_request(parameters_dic,instrument_name,query_status,job_id=job_id,upload_data=None)
        query_status = query_out['query_status']
        job_id = query_out['job_monitor']['job_id']
        time.sleep(5)



    #print ('job_id',job_id)
    #print('query_status', query_status)


    print ('\n\n')

    print('================ Final Query  Report =====================')
    print('')
    if query_status != 'failed':
        pass
    else:
        failure_report(query_out)
        raise Exception('query failed',query_out)


    print('exit_status, status', query_out['exit_status']['status'])
    print('exit_status, message', query_out['exit_status']['message'])
    print('exit_status, error_message', query_out['exit_status']['error_message'])
    print('exit_status, debug_message', query_out['exit_status']['debug_message'])
    print('job_monitor', query_out['job_monitor'])
    print('query_status', query_out['query_status'])
    print('products', query_out['products'].keys())
    for k in query_out['products'].keys():
        if k=='image':
            print (k, '=>',query_out['products'][k].keys())
        else:
            print(k, '=>',query_out['products'][k])


def failure_report(query_out):
    print('exit_status, status', query_out['exit_status']['status'])
    print('exit_status, message', query_out['exit_status']['message'])
    print('exit_status, error_message', query_out['exit_status']['error_message'])
    print('exit_status, debug_message', query_out['exit_status']['debug_message'])
    #try:
    #    tmp_res = json.load(open('tmp_response_content.txt'))
    #    print('-------tmp_response_content------- ')
    #    for k in tmp_res.keys():
    #        print(k, '=>', tmp_res[k])
    #    print()
    #except:
    #    pass

    #raise Exception('query failed', query_out)


def test_meta_data(instrument_name='isgri',):
    testapp = flask.Flask(__name__)



    with testapp.test_request_context(method='POST', content_type='multipart/form-data', data=None):
        query = InstrumentQueryBackEnd(instrument_name=instrument_name, get_meta_data=True)

        print('request', request.method)
        query_out = query.get_meta_data()

        print('\n\n\n')

        print('query_out:job_monitor', query_out)


