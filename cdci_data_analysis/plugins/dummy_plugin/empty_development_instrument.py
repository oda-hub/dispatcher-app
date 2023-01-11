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

__author__ = "Gabriele Barni"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.queries import SourceQuery, InstrumentQuery

from .data_server_dispatcher import (EmptyProductQuery, 
                                     DataServerNumericQuery, 
                                     FailingProductQuery, 
                                     DataServerParametricQuery, 
                                     EchoProductQuery)
from .empty_instrument import BoundaryFloat

# duplicated with jemx, but this staticmethod makes it complex.
# this all should be done commonly, for all parameters - limits are common thing
from ...analysis.parameters import SpectralBoundary, Angle, Energy


# class InstrumentWithCheckQuery(InstrumentQuery):
#     def check_instrument_roles(self, roles, email):
#         return True



def my_instr_factory():
    src_query = SourceQuery('src_query')

    # empty query
    instr_query = InstrumentQuery(name='empty_instrument_query',
                                           input_prod_list_name='p_list',
                                           catalog=None,
                                           catalog_name='user_catalog')

    empty_query = EmptyProductQuery('empty_parameters_dummy_query',)
    failing_query = FailingProductQuery('failing_parameters_dummy_query', )
    # let's build a simple parameter to its list
    p = BoundaryFloat(value=10., name='p', units='W',)
    numerical_query = DataServerNumericQuery('numerical_parameters_dummy_query',
                                             parameters_list=[p])

    # let's build a simple parameter to its list
    sb = SpectralBoundary(value=10., name='sb')
    parametrical_query = DataServerParametricQuery('parametrical_parameters_dummy_query',
                                                   parameters_list=[sb])

    ang = Angle(value=1., units='arcsec', default_units='arcsec', name='ang')
    ang_deg = Angle(value=1., units='deg', default_units='arcsec', name='ang_deg')
    energ = Energy(value=1., E_units='MeV', name='energ')
    echo_param_query = EchoProductQuery('echo_parameters_dummy_query',
                                        parameters_list=[ang, ang_deg, energ])

    query_dictionary = {'dummy': 'empty_parameters_dummy_query',
                        'numerical': 'numerical_parameters_dummy_query',
                        'failing': 'failing_parameters_dummy_query',
                        'parametrical': 'parametrical_parameters_dummy_query',
                        'echo': 'echo_parameters_dummy_query'}

    return Instrument('empty-development',
                      src_query=src_query,
                      restricted_access=True,
                      instrumet_query=instr_query,
                      product_queries_list=[empty_query, numerical_query, failing_query, parametrical_query, echo_param_query],
                      query_dictionary=query_dictionary)
