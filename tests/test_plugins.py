from __future__ import print_function
import  logging

logger = logging.getLogger(__name__)

from cdci_data_analysis.configurer import ConfigEnv
osaconf = ConfigEnv.from_conf_file('./conf_env.yml')

import time

from cdci_data_analysis.ddosa_interface.osa_catalog import OsaCatalog

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

    from cdci_data_analysis.ddosa_interface.osa_isgri import OSA_ISGRI
    from cdci_data_analysis.flask_app.app import set_session_logger
    instr= OSA_ISGRI()
    set_session_logger(out_dir)
    parameters_dic=dict(E1_keV=20.,E2_keV=40.,T1 =T1_iso, T2=T2_iso,RA=RA,DEC=DEC,radius=25,scw_list=None)

    logger.info('parameters dictionary')
    logger.info(parameters_dic)
    instr.set_pars_from_dic(parameters_dic)

    if use_scw_list==True:
        instr.set_par('scw_list',cookbook_scw_list)
    else:
        instr.set_par('scw_list', [])
        #instr.set_par('time_group_selector','time_range_iso')

    if use_catalog==True:
        osa_catalog = OsaCatalog.build_from_dict_list([dict(ra=RA, dec=DEC, name="TEST_SOURCE")])

        instr.set_par('user_catalog',osa_catalog)

    instr.show_parameters_list()

    prod_dictionary = instr.run_query('isgri_image_query', config=osaconf, out_dir=out_dir, query_type=query_type)

    print ('prod_dictionary',prod_dictionary)


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
                      scw_list=cookbook_scw_list,src_name='4U 1700-377')

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

    print ('prod_dictionary',prod_dictionary)


    if use_catalog==True:
        print("input catalog:",osa_catalog.name)
        #assert _names.header['NAME']==parameters['src_name']
        #TODO: we could also extract other sources really, and assert if the result is consistent with input.
        #TODO: (for better test coverage)



def test_fit_spectrum_cookbook():
    import xspec as xsp
    # PyXspec operations:
    s = xsp.Spectrum("spectrum.fits")
    s.ignore('**-15.')
    s.ignore('300.-**')
    xsp.Model("cutoffpl")
    xsp.Fit.query = 'yes'
    xsp.Fit.perform()

    xsp.Plot.device = "/xs"

    xsp.Plot.xLog = True
    xsp.Plot.yLog = True
    xsp.Plot.setRebin(10., 5)
    xsp.Plot.xAxis='keV'
    # Plot("data","model","resid")
    # Plot("data model resid")
    xsp.Plot("data,delchi")

    xsp.Plot.show()
    import matplotlib
    matplotlib.use('TkAgg')

    import pylab as plt
    fig, ax = plt.subplots()

    ax.set_xscale("log", nonposx='clip')
    ax.set_yscale("log")

    plt.errorbar(xsp.Plot.x(), xsp.Plot.y(), xerr=xsp.Plot.xErr(), yerr=xsp.Plot.yErr(), fmt='o')
    plt.step(xsp.Plot.x(), xsp.Plot.model(),where='mid')
    ax.set_xlabel('Energy (keV)')
    ax.set_ylabel('normalize counts  s$^{-1}$ keV$^{-1}$')
    plt.show()

def test_lightcurve_cookbook(use_scw_list=True,use_catalog=False,query_type='Real',out_dir=None):
    from cdci_data_analysis.ddosa_interface.osa_isgri import OSA_ISGRI
    from cdci_data_analysis.flask_app.app import set_session_logger
    set_session_logger(out_dir)

    instr = OSA_ISGRI()
    src_name = '4U==1700-377'
    parameters = dict(E1_keV=20., E2_keV=40., T1=T1_iso, T2=T2_iso, RA=RA, DEC=DEC, radius=25,
                      scw_list=cookbook_scw_list, src_name=src_name,time_bin=0.1,time_bin_format='jd')

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

    print (prod_dictionary)

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
    test_mosaic_cookbook(use_catalog=True, use_scw_list=True,out_dir='test_scratch',query_type='Real')
    #test_mosaic_cookbook(use_catalog=False, use_scw_list=False)
    #test_mosaic_cookbook(use_catalog=False, use_scw_list=True)


def test_full_spectrum():
    #test_spectrum_cookbook(use_catalog=True, use_scw_list=False)
    #test_spectrum_cookbook(use_catalog=True, use_scw_list=True)
    #test_spectrum_cookbook(use_catalog=False, use_scw_list=False)
    test_spectrum_cookbook(use_catalog=False, use_scw_list=True,query_type='Real',out_dir='test_scratch')

def test_full_lc():
    #test_lightcurve_cookbook(use_catalog=True, use_scw_list=False)
    test_lightcurve_cookbook(use_catalog=True, use_scw_list=True,out_dir='test_scratch',query_type='Real')
    #test_lightcurve_cookbook(use_catalog=False, use_scw_list=False)
    #test_lightcurve_cookbook(use_catalog=False, use_scw_list=True)


