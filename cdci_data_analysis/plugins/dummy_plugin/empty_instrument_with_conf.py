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

from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.queries import SourceQuery, InstrumentQuery

from .data_server_dispatcher import EmptyProductQueryNoScwList, DataServerQueryDispConf

def my_instr_factory():
    src_query = SourceQuery('src_query')

    # empty query
    instr_query = InstrumentQuery(name='empty_instrument_query',
                                  input_prod_list_name='p_list',
                                  catalog=None,
                                  catalog_name='user_catalog')

    empty_query = EmptyProductQueryNoScwList('empty_parameters_dummy_query',)

    query_dictionary = {'dummy': 'empty_parameters_dummy_query'}

    return Instrument('empty-with-conf',
                      src_query=src_query,
                      instrumet_query=instr_query,
                      product_queries_list=[empty_query,],
                      query_dictionary=query_dictionary,
                      data_server_query_class=DataServerQueryDispConf)
