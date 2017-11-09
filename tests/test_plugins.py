from __future__ import print_function
from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)



import  logging

logger = logging.getLogger(__name__)

from cdci_data_analysis.configurer import ConfigEnv
osaconf = ConfigEnv.from_conf_file('./conf_env.yml')

import time
from flask import Flask, request
import flask

from cdci_data_analysis.ddosa_interface.osa_catalog import OsaCatalog

from cdci_data_analysis.flask_app.app import InstrumentQueryBackEnd




crab_scw_list=["035200230010.001","035200240010.001"]
cookbook_scw_list=['005100410010.001','005100420010.001','005100430010.001','005100440010.001','005100450010.001'][:2]
single_scw_list=['005100410010.001']

T1_iso='2003-03-15T23:27:40.0'
T2_iso='2003-03-16T00:03:15.0'

RA=257.815417
DEC=-41.593417


def test_instr(use_scw_list=True):

    from cdci_data_analysis.ddosa_interface.osa_isgri import OSA_ISGRI

    instr= OSA_ISGRI()

    parameters_dic=dict(E1_keV=20.,E2_keV=40.,T1=T1_iso, T2=T2_iso,RA=RA,DEC=DEC,radius=25,scw_list=None,T_format='isot')


    instr.set_pars_from_dic(parameters_dic)

    if use_scw_list==True:
        instr.set_par('scw_list',cookbook_scw_list)
    else:
        instr.set_par('scw_list', [])
        instr.set_par('time_group_selector','time_range_iso')



def test_mosaic_cookbook(use_scw_list=False,use_catalog=False,query_type='Real',out_dir='./'):


    testapp = flask.Flask(__name__)

    if use_scw_list==True:
        scw_list=cookbook_scw_list
    else:
        scw_list=None

    if use_catalog == True:
        cat_dict={}
        cat_dict['cat_column_list']=[[RA],[DEC],['TEST_SOURCE'],[10.]]
        cat_dict['cat_column_names']=['ra','dec','src_names','significance']
        cat_dict['cat_frame']='fk5'
        cat_dict['cat_coord_units']='deg'
        cat_dict['cat_lon_name']='ra'
        cat_dict['cat_lat_name'] = 'dec'
    else:
        cat_dict=None

    parameters_dic=dict(E1_keV=20.,E2_keV=40.,T1=T1_iso, T2=T2_iso,RA=RA,DEC=DEC,radius=25,scw_list=scw_list,
                        image_scale_min=1,session_id='test',query_type=query_type,product_type='isgri_image',
                        detection_threshold=5.0,user_catalog_dictionary=None)
    data_cat_fits = dict(
        user_catalog_file=(open("dummy_prods/query_catalog.fits", "rb", buffering=0), "query_user_catalog"),
    )

    data_cat_csv = dict(
        user_catalog_file=(open("dummy_prods/query_catalog.txt", "rb", buffering=0), "query_user_catalog"),
    )

    data_scw_list = dict(
        user_scw_list_file=(open("dummy_prods/query_scw_list.txt", "rb", buffering=0), "query_scw_list"),
    )


    with testapp.test_request_context( method='POST',content_type='multipart/form-data',data=None):

        instrument_name = 'ISGRI'
        query = InstrumentQueryBackEnd(instrument_name=instrument_name,par_dic=parameters_dic,config=osaconf)




        query.instrument.show_parameters_list()
        print ('request',request.method)
        query_out=query.run_query(off_line=False)
        #print(query_out.prod_dictionary)
        print('\n\n\n')

        print('status',query_out.status_dictionary['status'])
        print('error_message',query_out.status_dictionary['error_message'])
        print('debug_message',query_out.status_dictionary['debug_message'])
        print('scw_list', query_out.prod_dictionary['input_prod_list'])
        assert query_out.status_dictionary['status']==0


def test_plot_mosaic():
    from astropy.io import fits as pf
    data= pf.getdata('mosaic.fits')
    import pylab as plt
    plt.imshow(data,interpolation='nearest')
    plt.show()




def test_spectrum_cookbook(use_scw_list=True,use_catalog=False,query_type='Real',out_dir=None):
    from cdci_data_analysis.ddosa_interface.osa_isgri import OSA_ISGRI
    from cdci_data_analysis.flask_app.app import set_session_logger

    instr = OSA_ISGRI()
    set_session_logger(out_dir)
    parameters = dict(E1_keV=20., E2_keV=40., T1 =T1_iso, T2 =T2_iso, RA=RA, DEC=DEC, radius=25,
                      scw_list=cookbook_scw_list,src_name='4U 1700-377',xspec_model='powerlaw')

    logger.info('parameters dictionary')
    logger.info(parameters)

    instr.set_pars_from_dic(parameters)


    if use_scw_list == True:
        instr.set_par('scw_list', cookbook_scw_list)
    else:
        instr.set_par('scw_list', [])
        #instr.set_par('time_group_selector', 'time_range_iso')

    if use_catalog==True:
        dra=float(time.strftime("0.%j")) # it's vital to make sure that the test changes with the phase of the moon
        ddec = float(time.strftime("0.%H%M%S"))

        dsrc_name="RD_%.6lg_%.6lg"%(RA+dra,DEC+ddec) # non-astronomical, fix
        osa_catalog = OsaCatalog.build_from_dict_list([
            dict(ra=RA, dec=DEC, name=parameters['src_name']),
            dict(ra=RA+dra, dec=DEC+ddec, name=dsrc_name)
        ])
        instr.set_par('user_catalog', osa_catalog)

    instr.show_parameters_list()

    prod_dictionary = instr.run_query('isgri_spectrum_query',config=osaconf,out_dir=out_dir,query_type=query_type)



    if use_catalog==True:
        print("input catalog:",osa_catalog.name)
        #assert _names.header['NAME']==parameters['src_name']
        #TODO: we could also extract other sources really, and assert if the result is consistent with input.
        #TODO: (for better test coverage)

    for k in prod_dictionary.keys():
        print(k,':', prod_dictionary[k])
        print ('\n')


def test_fit_spectrum_cookbook(use_catalog=False,query_type='Real',out_dir=None):
    from cdci_data_analysis.ddosa_interface.osa_isgri import OSA_ISGRI
    from cdci_data_analysis.flask_app.app import set_session_logger

    instr = OSA_ISGRI()
    print ('out_dir',out_dir)
    set_session_logger(out_dir)
    parameters = dict(E1_keV=20., E2_keV=40., T1 =T1_iso, T2 =T2_iso, RA=RA, DEC=DEC, radius=25,
                      scw_list=cookbook_scw_list,src_name='4U 1700-377',xspec_model='powerlaw',
                      ph_file='query_spectrum_isgri_sum_1E_1740.7-2942.fits',
                      arf_file='query_spectrum_arf_sum_1E_1740.7-2942.fits.gz',
                      rmf_file='query_spectrum_rmf_sum_1E_1740.7-2942.fits.gz')

    logger.info('parameters dictionary')
    logger.info(parameters)

    instr.set_pars_from_dic(parameters)




    if use_catalog==True:
        dra=float(time.strftime("0.%j")) # it's vital to make sure that the test changes with the phase of the moon
        ddec = float(time.strftime("0.%H%M%S"))

        dsrc_name="RD_%.6lg_%.6lg"%(RA+dra,DEC+ddec) # non-astronomical, fix
        osa_catalog = OsaCatalog.build_from_dict_list([
            dict(ra=RA, dec=DEC, name=parameters['src_name']),
            dict(ra=RA+dra, dec=DEC+ddec, name=dsrc_name)
        ])
        instr.set_par('', osa_catalog)

    instr.show_parameters_list()

    prod_dictionary = instr.run_query('spectral_fit_query',config=osaconf,out_dir=out_dir,query_type=query_type)



    if use_catalog==True:
        print("input catalog:",osa_catalog.name)
        #assert _names.header['NAME']==parameters['src_name']
        #TODO: we could also extract other sources really, and assert if the result is consistent with input.
        #TODO: (for better test coverage)

    for k in prod_dictionary.keys():
        print(k,':', prod_dictionary[k])
        print ('\n')

def test_lightcurve_cookbook(use_scw_list=True,use_catalog=False,query_type='Real',out_dir=None):
    from cdci_data_analysis.ddosa_interface.osa_isgri import OSA_ISGRI
    from cdci_data_analysis.flask_app.app import set_session_logger
    set_session_logger(out_dir)

    instr = OSA_ISGRI()
    src_name = '4U==1700-377'
    parameters = dict(E1_keV=20., E2_keV=40., T1=T1_iso, T2=T2_iso, RA=RA, DEC=DEC, radius=25,
                      scw_list=cookbook_scw_list, src_name=src_name,time_bin=100,time_bin_format='sec')

    logger.info('parameters dictionary')
    logger.info(parameters)


    instr.set_pars_from_dic(parameters)

    logger.info(instr.get_parameters_list_as_json()  )

    if use_scw_list == True:
        instr.set_par('scw_list', cookbook_scw_list)
    else:
        instr.set_par('scw_list', [])

    if use_catalog==True:
        dra=float(time.strftime("0.%j")) # it's vital to make sure that the test changes with the phase of the moon
        ddec = float(time.strftime("0.%H%M%S"))

        dsrc_name="RD_%.6lg_%.6lg"%(RA+dra,DEC+ddec) # non-astronomical, fix
        osa_catalog = OsaCatalog.build_from_dict_list([
            dict(ra=RA, dec=DEC, name=parameters['src_name']),
            dict(ra=RA+dra, dec=DEC+ddec, name=dsrc_name)
        ])
        instr.set_par('user_catalog', osa_catalog)

    instr.show_parameters_list()

    prod_dictionary = instr.run_query('isgri_lc_query', config=osaconf, out_dir=out_dir, query_type=query_type)

    #instr.get_query_by_name('isgri_lc_query').get_prod_by_name('isgri_lc').get_html_draw(plot=True)



def test_plot_lc():
    from astropy.io import fits as pf
    data= pf.getdata('lc.fits',ext=1)

    import matplotlib
    matplotlib.use('TkAgg')
    import pylab as plt
    fig, ax = plt.subplots()

    #ax.set_xscale("log", nonposx='clip')
    #ax.set_yscale("log")

    plt.errorbar(data['TIME'], data['RATE'], yerr=data['ERROR'], fmt='o')
    ax.set_xlabel('Time ')
    ax.set_ylabel('Rate ')
    plt.show()



def test_full_mosaic():
    #test_mosaic_cookbook(use_catalog=True,use_scw_list=False)
    test_mosaic_cookbook(use_catalog=True, use_scw_list=False,out_dir='test_scratch',query_type='Real')
    #test_mosaic_cookbook(use_catalog=False, use_scw_list=False)
    #test_mosaic_cookbook(use_catalog=False, use_scw_list=True)


def test_full_spectrum():
    #test_spectrum_cookbook(use_catalog=True, use_scw_list=False)
    #test_spectrum_cookbook(use_catalog=True, use_scw_list=True)
    #test_spectrum_cookbook(use_catalog=False, use_scw_list=False)
    test_spectrum_cookbook(use_catalog=False, use_scw_list=True,query_type='Dummy',out_dir='test_scratch')

def test_full_fit_spectrum():
    test_fit_spectrum_cookbook(use_catalog=False,query_type='Dummy',out_dir='test_scratch')

def test_full_lc():
    #test_lightcurve_cookbook(use_catalog=True, use_scw_list=False)
    test_lightcurve_cookbook(use_catalog=True, use_scw_list=True,out_dir='test_scratch',query_type='Real')
    #test_lightcurve_cookbook(use_catalog=False, use_scw_list=False)
    #test_lightcurve_cookbook(use_catalog=False, use_scw_list=True)


