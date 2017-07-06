  
from cdci_data_analysis.configurer import ConfigEnv
osaconf = ConfigEnv.from_conf_file('./conf_env.yml')

cookbook_scw_list=['005100410010.001','005100420010.001','005100430010.001','005100440010.001','005100450010.001']
crab_scw_list=["035200230010.001","035200240010.001"]

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



def test_mosaic_cookbook():
    from cdci_data_analysis.ddosa_interface.osa_image_dispatcher import OSA_ISGRI_IMAGE

    prod= OSA_ISGRI_IMAGE()

    parameters=dict(E1=20.,E2=40.,T1="2008-04-12T11:11:11.0",T2="2009-04-12T11:11:11.0",RA=83,DEC=22,radius=5,scw_list=cookbook_scw_list)

    for p,v in parameters.items():
        print('set from form',p,v)
        prod.set_par_value(p, v)
        print('--')
    prod.set_par_value('time_group_selector','scw_list')
    prod.show_parameters_list()

    out_prod, exception=prod.get_product(config=osaconf)

    print('out_prod', out_prod,exception)

    print dir(out_prod)
    from astropy.io import fits as pf
    pf.writeto('mosaic.fits',out_prod,overwrite=True)
    assert sum(out_prod.flatten()>0)>100 # some non-zero pixels

def test_mosaic_cookbook_one_scw():


    from cdci_data_analysis.ddosa_interface.osa_image_dispatcher import OSA_ISGRI_IMAGE

    prod= OSA_ISGRI_IMAGE()

    parameters=dict(E1=20.,E2=40.,T1="2008-04-12T11:11:11.0",T2="2009-04-12T11:11:11.0",RA=83,DEC=22,radius=5,scw_list=cookbook_scw_list[1:])

    for p,v in parameters.items():
        print('set from form',p,v)
        prod.set_par_value(p, v)
        print('--')
    prod.set_par_value('time_group_selector','scw_list')
    prod.show_parameters_list()

    out_prod, exception=prod.get_product(config=osaconf)

    print('out_prod', out_prod,exception)

    print dir(out_prod)

    from astropy.io import fits as pf
    pf.writeto('mosaic.fits', out_prod, overwrite=True)
    assert sum(out_prod.flatten()>0)>100 # some non-zero pixels


def test_plot_mosaic():
    from astropy.io import fits as pf
    data= pf.getdata('mosaic.fits')
    import pylab as plt
    plt.imshow(data,interpolation='nearest')
    plt.show()


def test_spectrum_cookbook():
    from cdci_data_analysis.ddosa_interface.osa_spectrum_dispatcher import OSA_ISGRI_SPECTRUM

    prod= OSA_ISGRI_SPECTRUM()

    parameters = dict(E1=20., E2=40., T1="2008-11-11T11:11:11.0", T2="2008-11-11T11:11:11.0", RA=83, DEC=22, radius=5,
                      scw_list=cookbook_scw_list,src_name='4U 1700-377')

    for p,v in parameters.items():
        print('set from form',p,v)
        prod.set_par_value(p, v)
        print('--')
    prod.set_par_value('time_group_selector','scw_list')
    prod.show_parameters_list()

    spectrum, rmf, arf, exception=prod.get_product(config=osaconf)
    #print ('spectrum',spectrum)
    #print out_prod,exception
    from astropy.io import fits as pf
    ##pf.writeto('spectrum.fits', out_prod, overwrite=True)
    #import os
    #path=os.path.dirname(out_prod)
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

    import pylab as plt
    fig, ax = plt.subplots()

    ax.set_xscale("log", nonposx='clip')
    ax.set_yscale("log")

    plt.errorbar(xsp.Plot.x(), xsp.Plot.y(), xerr=xsp.Plot.xErr(), yerr=xsp.Plot.yErr(), fmt='o')
    plt.step(xsp.Plot.x(), xsp.Plot.model(),where='mid')
    ax.set_xlabel('Energy (keV)')
    ax.set_ylabel('normalize counts  s$^{-1}$ keV$^{-1}$')
    plt.show()

def test_lightcurve_cookbook():
    from cdci_data_analysis.ddosa_interface.osa_lightcurve_dispatcher import OSA_ISGRI_LIGHTCURVE

    prod= OSA_ISGRI_LIGHTCURVE()

    parameters = dict(E1=20., E2=40., T1="2008-11-11T11:11:11.0", T2="2008-11-11T11:11:11.0", scw_list=cookbook_scw_list[1:2],src_name='4U 0517+17')

    for p,v in parameters.items():
        print('set from form',p,v)
        prod.set_par_value(p, v)
        print('--')
    prod.set_par_value('time_group_selector','scw_list')
    prod.show_parameters_list()

    out_prod, exception=prod.get_product(config=osaconf)
    if out_prod is None:
        raise RuntimeError('no light curve produced')
    print ('out_prod',dir(out_prod))

    from astropy.io import fits as pf
    pf.writeto('lc.fits', out_prod, overwrite=True)


def test_plot_lc():
    from astropy.io import fits as pf
    data= pf.getdata('lc.fits')


    import pylab as plt
    fig, ax = plt.subplots()

    #ax.set_xscale("log", nonposx='clip')
    #ax.set_yscale("log")

    plt.errorbar(data['TIME'], data['RATE'], yerr=data['ERROR'], fmt='o')
    ax.set_xlabel('Time ')
    ax.set_ylabel('Rate ')
    plt.show()