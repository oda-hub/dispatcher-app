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

from cdci_data_analysis.analysis.instrument import Instrument

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f

from astropy.io import  fits as pf


from ..analysis.queries import  *
from ..analysis.products import *
from .osa_image_dispatcher import IsgriMosaicQuery
from .osa_spectrum_dispatcher import IsgriSpectrumQuery
from .osa_lightcurve_dispatcher import IsgriLightCurveQuery
from .osa_dispatcher import OsaQuery





def OSA_ISGRI():

    src_query=SourceQuery('src_query')

    instr_query=InstrumentQuery(
        name='isgri_parameters',
        radius_name='radius',
        raidus_units='deg',
        radius_value=5.0,
        E1_name='E1_keV',
        E1_units='keV',
        E1_value=10.,
        E2_name='E2_keV',
        E2_units='keV',
        E2_value=40.,
        input_prod_list_name='scw_list',
        input_prod_value=None,
        catalog=None,
        catalog_name='user_catalog')





    light_curve =IsgriLightCurveQuery('isgri_lc_query')

    image=IsgriMosaicQuery('isgri_image_query')

    spectrum=IsgriSpectrumQuery('isgri_spectrum_query')

    xspec_fit = SpectralFitQuery('spectral_fit_query', None)


    query_dictionary={}
    query_dictionary['isgri_image'] = 'isgri_image_query'
    query_dictionary['isgri_spectrum'] = 'isgri_spectrum_query'
    query_dictionary['isgri_lc'] = 'isgri_lc_query'
    query_dictionary['spectral_fit'] = 'spectral_fit_query'

    return  Instrument('ISGRI',
                       src_query=src_query,
                       instrumet_query=instr_query,
                       #input_product_query=input_data,
                       product_queries_list=[image,spectrum,light_curve,xspec_fit],
                       data_server_query_class=OsaQuery,
                       query_dictionary=query_dictionary,
                       max_pointings=50)
