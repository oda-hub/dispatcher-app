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

from .data_server_dispatcher import FileParameterQuery

from ...analysis.parameters import POSIXPath

def my_instr_factory():
    src_query = SourceQuery('src_query')

    instr_query = InstrumentQuery(name='empty_instrument_query',
                                  input_prod_list_name='p_list',
                                  catalog=None,
                                  catalog_name='user_catalog')

    f = POSIXPath(value=None, name='dummy_POSIX_file', is_optional=True)
    file_query = FileParameterQuery('file_parameters_dummy_query',
                                    parameters_list=[f])

    query_dictionary = {'file_dummy': 'file_parameters_dummy_query'}

    return Instrument('empty-with-posix-path',
                      src_query=src_query,
                      instrumet_query=instr_query,
                      product_queries_list=[file_query,],
                      query_dictionary=query_dictionary)
