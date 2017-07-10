  
from cdci_data_analysis.configurer import ConfigEnv
osaconf = ConfigEnv.from_conf_file('./conf_env.yml')


crab_scw_list=["035200230010.001","035200240010.001"]
cookbook_scw_list=['005100410010.001','005100420010.001','005100430010.001','005100440010.001','005100450010.001']
single_scw_list=['005100410010.001']

T_start='2003-03-15T23:27:40.0'
T_stop='2003-03-16T00:03:15.0'

RA=257.815417
DEC=-41.593417


def test_too_strickt_type_verifications():
    from cdci_data_analysis.ddosa_interface.osa_image_dispatcher import OSA_ISGRI_IMAGE

    parameters=dict(E1=20,E2=40,T1="2008-11-11T11:11:11.0",T2="2008-11-11T11:11:11.0")
    
    prod= OSA_ISGRI_IMAGE()
    for p,v in parameters.items():
        print('set from form',p,v)
        prod.set_par_value(p, v)
        print('--')
    prod.set_par_value('time_group_selector','scw_list')
    prod.show_parameters_list()



def test_mosaic_cookbook(use_scw_list=True):
    from cdci_data_analysis.ddosa_interface.osa_image_dispatcher import OSA_ISGRI_IMAGE

    prod= OSA_ISGRI_IMAGE()

    parameters=dict(E1=20.,E2=40.,T1=T_start, T2=T_stop,RA=RA,DEC=DEC,radius=25,scw_list=cookbook_scw_list)

    for p,v in parameters.items():
        print('set from form',p,v)
        prod.set_par_value(p, v)
        print('--')
    if use_scw_list==True:
        prod.set_par_value('time_group_selector','scw_list')
    else:
        prod.set_par_value('time_group_selector', 'time_range_iso')
    prod.show_parameters_list()

    image,catalog, exception=prod.get_product(config=osaconf)

    print('out_prod', image,exception)

    print dir(image)
    from astropy.io import fits as pf
    pf.writeto('mosaic.fits',image,overwrite=True)
    pf.writeto('mosaic_catalog.fits', catalog, overwrite=True)
    assert sum(image.flatten()>0)>100 # some non-zero pixels



def test_plot_mosaic():
    from astropy.io import fits as pf
    data= pf.getdata('mosaic.fits')
    import pylab as plt
    plt.imshow(data,interpolation='nearest')
    plt.show()


def test_spectrum_cookbook(use_scw_list=True):
    from cdci_data_analysis.ddosa_interface.osa_spectrum_dispatcher import OSA_ISGRI_SPECTRUM

    prod= OSA_ISGRI_SPECTRUM()

    parameters = dict(E1=20., E2=40., T1=T_start, T2=T_stop, RA=RA, DEC=DEC, radius=25,
                      scw_list=cookbook_scw_list,src_name='4U 1700-377')

    for p,v in parameters.items():
        print('set from form',p,v)
        prod.set_par_value(p, v)
        print('--')

    if use_scw_list==True:
        prod.set_par_value('time_group_selector','scw_list')
    else:
        prod.set_par_value('time_group_selector', 'time_range_iso')

    prod.show_parameters_list()

    spectrum, rmf, arf, exception=prod.get_product(config=osaconf)
    #print ('spectrum',spectrum)
    #print out_prod,exception
    from astropy.io import fits as pf
    ##pf.writeto('spectrum.fits', out_prod, overwrite=True)
    #import os
    #path=os.path.dirname(out_prod)
    if spectrum is None:
        raise RuntimeError('no light curve produced')
    spectrum[1].header['RESPFILE']='rmf.fits'
    spectrum[1].header['ANCRFILE']='arf.fits'
    spectrum.writeto('spectrum.fits',overwrite=True)
    rmf.writeto('rmf.fits',overwrite=True)
    arf.writeto('arf.fits',overwrite=True)
    print ('dir prod',dir(spectrum))



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

def test_lightcurve_cookbook(use_scw_list=True):
    from cdci_data_analysis.ddosa_interface.osa_lightcurve_dispatcher import OSA_ISGRI_LIGHTCURVE

    prod= OSA_ISGRI_LIGHTCURVE()

    parameters = dict(E1=20., E2=40.,T1=T_start, T2=T_stop, RA=RA, DEC=DEC, radius=25, scw_list=cookbook_scw_list,src_name="4U 1700-377")

    for p,v in parameters.items():
        print('set from form',p,v)
        prod.set_par_value(p, v)
        print('--')
    if use_scw_list == True:
        prod.set_par_value('time_group_selector', 'scw_list')
    else:
        prod.set_par_value('time_group_selector', 'time_range_iso')
    prod.show_parameters_list()

    out_prod, exception=prod.get_product(config=osaconf)
    if out_prod is None:
        raise RuntimeError('no light curve produced')
    print ('out_prod',dir(out_prod))

    from astropy.io import fits as pf
    pf.writeto('lc.fits', out_prod, overwrite=True)


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
    test_mosaic_cookbook()
    #test_plot_mosaic()
    #test_mosaic_cookbook(use_scw_list=False)


def test_full_spectrum():
    test_spectrum_cookbook()
    #test_spectrum_cookbook(use_scw_list=False)

def test_full_lc():
    test_lightcurve_cookbook()
    #test_lightcurve_cookbook(use_scw_list=False)