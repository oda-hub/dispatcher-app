from __future__ import absolute_import, division, print_function

__author__ = "Gabriele Barni"

from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.queries import SourceQuery, InstrumentQuery, Float

from .data_server_dispatcher import DataServerQuery, ReturnProgressProductQuery


def my_instr_factory():
    src_query = SourceQuery('src_query')

    # empty query
    instr_query = InstrumentQuery(name='empty_async_return_progress_instrument_query',
                                  input_prod_list_name='scw_list',
                                  catalog=None,
                                  catalog_name='user_catalog')

    p = Float(value=10., name='p', units='W')
    return_progress_query = ReturnProgressProductQuery('empty_parameters_dummy_query_return_progress',
                                                       parameters_list=[p])

    query_dictionary = {'dummy': 'empty_parameters_dummy_query_return_progress'}

    return Instrument('empty-async-return-progress',
                      src_query=src_query,
                      instrumet_query=instr_query,
                      product_queries_list=[return_progress_query],
                      query_dictionary=query_dictionary,
                      data_server_query_class=DataServerQuery)
