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

from ..analysis.products import  *
from .osa_lightcurve_dispatcher import get_osa_lightcurve
from .osa_image_dispatcher import get_osa_image
from .osa_spectrum_dispatcher import get_osa_spectrum

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
        input_prod_value=None)

    light_curve =LightCurve('isgri_lc',None)
    image=Image('isgri_image',None,get_product_method=get_osa_image)
    spectrum=Spectrum('isgri_spectrum',None,get_product_method=get_osa_spectrum)

    return  Instrument('ISGRI',
                       src_query=src_query,
                       instrumet_query=instr_query,
                       product_queries_list=[image,spectrum,light_curve])
