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
from .osa_dispatcher import    OsaQuery,QueryProduct
from ..analysis.products import Spectrum
from ..web_display import draw_spectrum
from astropy.io import  fits as pf




def do_spectrum_from_scw_list(E1,E2,scw_list=["035200230010.001","035200240010.001"]):
    dic_str = str(scw_list[0])
    target="ii_spectra_extract"
    modules=["ddosa", "git://ddosadm"]
    assume=['ddosa.ScWData(input_scwid="%s")'%dic_str,\
                       'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),\
                       'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")']

    return QueryProduct(target=target, modules=modules, assume=assume)


def do_spectrum_from_time_span(E1,E2,T1,T2):
    target="ii_spectra_extract"
    modules=["ddosa", "git://ddosadm"]
    assume=['ddosa.ScWImageList(input_scwlist=rangequery.TimeDirectionScWList(use_coordinates=dict(RA=83,DEC=22,radius=5),\
        use_timespan=dict(T1=%(T1)s,T2=%(T2)s)),\
        use_max_pointings=3)'%dict(T1=T1,T2=T2),\
        'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),\
        'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")']

    return QueryProduct(target=target, modules=modules, assume=assume)


def get_osa_spectrum(analysis_prod,dump_json=False,use_dicosverer=False,config=None):

    q=OsaQuery(config=config)

    time_range_type = analysis_prod.get_par_by_name('time_group_selector').value

    if time_range_type == 'scw_list':

        query_prod = do_spectrum_from_scw_list(analysis_prod.get_par_by_name('E1').value,
                                      analysis_prod.get_par_by_name('E2').value,
                                      analysis_prod.get_par_by_name('scw_list').value)

    elif time_range_type == 'time_range_iso':
        query_prod = do_spectrum_from_time_span( analysis_prod.get_par_by_name('E1').value,
                                       analysis_prod.get_par_by_name('E2').value,
                                       analysis_prod.get_par_by_name('T1').value,
                                       analysis_prod.get_par_by_name('T2').value)
    else:
        raise RuntimeError('wrong time format')

    res = q.run_query(query_prod=query_prod)

    data, prod_path, e = q.get_data(res, 'spectrum')
    spectrum = pf.open(prod_path)
    print ('prod path',prod_path)

    return spectrum, e

def OSA_ISGRI_SPECTRUM():
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

    return Spectrum(parameters_list, get_product_method=get_osa_spectrum,html_draw_method=draw_spectrum)