  
from cdci_data_analysis.configurer import ConfigEnv
osaconf = ConfigEnv.from_conf_file('./conf_env.yml')

cookbook_scw_list=['005100410010.001','005100420010.001','005100430010.001','005100440010.001','005100450010.001'][:2]
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




def test_spectrum_cookbook():
    from cdci_data_analysis.ddosa_interface.osa_spectrum_dispatcher import OSA_ISGRI_SPECTRUM

    prod= OSA_ISGRI_SPECTRUM()

    parameters = dict(E1=20., E2=40., T1="2008-11-11T11:11:11.0", T2="2008-11-11T11:11:11.0", RA=83, DEC=22, radius=5,
                      scw_list=cookbook_scw_list)

    for p,v in parameters.items():
        print('set from form',p,v)
        prod.set_par_value(p, v)
        print('--')
    prod.set_par_value('time_group_selector','scw_list')
    prod.show_parameters_list()

    out_prod, exception=prod.get_product(config=osaconf)

    print out_prod,exception
    from astropy.io import fits as pf
    ##pf.writeto('spectrum.fits', out_prod, overwrite=True)
    print dir(out_prod)



def test_lightcurve_cookbook():
    from cdci_data_analysis.ddosa_interface.osa_lightcurve_dispatcher import OSA_ISGRI_LIGHTCURVE

    prod= OSA_ISGRI_LIGHTCURVE()

    parameters = dict(E1=20., E2=40., T1="2008-11-11T11:11:11.0", T2="2008-11-11T11:11:11.0", RA=83, DEC=22, radius=5, scw_list=cookbook_scw_list)

    for p,v in parameters.items():
        print('set from form',p,v)
        prod.set_par_value(p, v)
        print('--')
    prod.set_par_value('time_group_selector','scw_list')
    prod.show_parameters_list()

    out_prod, exception=prod.get_product(config=osaconf)

    print out_prod,exception

    print dir(out_prod)
