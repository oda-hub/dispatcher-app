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

__author__ = "Andrea Tramacere"


from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.queries import  *

from .data_server_dispatcher import DataServerQuery
from .image_query import MyInstrMosaicQuery

def my_instr_factory():
    src_query = SourceQuery('src_query')

    max_pointings = Integer(value=50, name='max_pointings')

    radius = Angle(value=5.0, units='deg', name='radius')
    E1_keV = SpectralBoundary(value=10., E_units='keV', name='E1_keV')
    E2_keV = SpectralBoundary(value=40., E_units='keV', name='E2_keV')
    spec_window = ParameterRange(E1_keV, E2_keV, 'spec_window')

    instr_query_pars = [radius, max_pointings, spec_window]

    instr_query = InstrumentQuery(
        name='my_instr_parameters',
        extra_parameters_list=instr_query_pars,
        input_prod_list_name='scw_list',
        input_prod_value=None,
        catalog=None,
        catalog_name='user_catalog')

    #
    #my_instr_image_query -> name given to this query
    image= MyInstrMosaicQuery('my_instr_image_query')

    # this dicts binds the product query name to the product name from frontend
    # eg my_instr_image is the parameter passed by the fronted to access the
    # the MyInstrMosaicQuery, and the dictionary will bing
    query_dictionary={}
    query_dictionary['my_instr_image'] = 'my_instr_image_query'

    return  Instrument('OSA_MYINSTR',
                       src_query=src_query,
                       instrumet_query=instr_query,
                       product_queries_list=[image],
                       data_server_query_class=DataServerQuery,
                       query_dictionary=query_dictionary,
                       max_pointings=50)

