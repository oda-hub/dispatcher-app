"""
Overview
--------
   
general info about this module


Classes and Inheritance Structure
----------------------------------------------
.. inheritance-diagram:: 

Summary
---------
.. autosummary::
   list of the module you want
    
Module API
----------
"""

from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Andrea Tramacere"



# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


import ddosaclient as dc


# Project
# relative import eg: from .mod import f
from ..analysis.parameters import *
from .osa_dispatcher import OsaQuery,QueryProduct
from ..analysis.products import LightCurve
#from ..web_display import draw_spectrum
from astropy.io import  fits as pf


def do_lightcurve_from_single_scw(image_E1,image_E2,time_bin_seconds,scw):
    """
    builds a spectrum for single scw

    * spectrum is built from image with one_bin mode
    * catalog default catalog is used for the image
    * ddosa selection is applied to build catalog for spectra


    :param image_E1:
    :param image_E2:
    :param scw:
    :return:
    """
    scw_str = str(scw)
    scwsource_module = "ddosa"
    target = "ii_lc_extract"
    modules = ["ddosa", "git://ddosadm"]
    assume = [scwsource_module + '.ScWData(input_scwid="%")'%scw_str,
             'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")'%dict(E1=image_E1,E2=image_E2),
             'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")',
             'ddosa.LCTimeBin(use_time_bin_seconds=100)']


def get_osa_lightcurve(analysis_prod,dump_json=False,use_dicosverer=False,config=None):

    q=OsaQuery(config=config)

    time_range_type = analysis_prod.get_par_by_name('time_group_selector').value
    #RA = analysis_prod.get_par_by_name('RA').value
    #DEC = analysis_prod.get_par_by_name('DEC').value
    #radiuse=analysis_prod.get_par_by_name('radius').value
    if time_range_type == 'scw_list':

        if len(analysis_prod.get_par_by_name('scw_list').value) == 1:
            query_prod = do_lightcurve_from_single_scw(analysis_prod.get_par_by_name('E1').value,
                                                     analysis_prod.get_par_by_name('E2').value,
                                                     scw_list=analysis_prod.get_par_by_name('scw_list').value[0])
        else:
            raise NotImplemented()


    elif time_range_type == 'time_range_iso':
        raise NotImplemented()

    else:
        raise RuntimeError('wrong time format')

    res = q.run_query(query_prod=query_prod)

    print(dir(res))

    for source_name,spec_attr,rmf_attr,arf_attr in res.extracted_sources:
        spectrum = pf.open(getattr(res,spec_attr))
        break # first one for now

    return spectrum, None

def OSA_ISGRI_LIGHTCURVE():
    E1_keV = Energy('keV', 'E1', value=20.0)
    E2_keV = Energy('keV', 'E2', value=40.0)

    E_range_keV = ParameterRange(E1_keV, E2_keV, 'E_range')

    t1_iso = Time('iso', 'T1', value='2001-12-11T00:00:00.0')
    t2_iso = Time('iso', 'T2', value='2001-12-11T00:00:00.0')

    t1_mjd = Time('mjd', 'T1_mjd', value=1.0)
    t2_mjd = Time('mjd', 'T2_mjd', value=1.0)

    t_range_iso = ParameterRange(t1_iso, t2_iso, 'time_range_iso')
    t_range_mjd = ParameterRange(t1_mjd, t2_mjd, 'time_range_mjd')

    scw_list = Time('prod_list', 'scw_list', value=['035200230010.001', '035200240010.001'])

    time_group = ParameterGroup([t_range_iso, t_range_mjd, scw_list], 'time_range', selected='scw_list')

    time_group_selector = time_group.build_selector('time_group_selector')

    E_cut = Energy('keV', 'E_cut', value=0.1)
    parameters_list = [E_range_keV, time_group, time_group_selector, scw_list, E_cut]

    return LightCurve(parameters_list, get_product_method=get_osa_lightcurve,html_draw_method=lambda *a:None)
