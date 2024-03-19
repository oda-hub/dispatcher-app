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
from cdci_data_analysis.analysis.queries import SourceQuery, InstrumentQuery, Float

from .data_server_dispatcher import (EmptyProductQuery, 
                                     DataServerNumericQuery, 
                                     FailingProductQuery, 
                                     DataServerParametricQuery, 
                                     EchoProductQuery,
                                     StructuredEchoProductQuery,
                                     FileParameterQuery)

# duplicated with jemx, but this staticmethod makes it complex.
# this all should be done commonly, for all parameters - limits are common thing
from ...analysis.exceptions import RequestNotUnderstood
from ...analysis.parameters import (SpectralBoundary, 
                                    Angle, 
                                    Energy, 
                                    Integer, 
                                    Float, 
                                    String,
                                    StructuredParameter,
                                    FileReference)



class BoundaryFloat(Float):
    def check_value(self):
        super().check_value()
        if self.value > 800:
            raise RequestNotUnderstood('p value is restricted to 800 W')


def my_instr_factory():
    src_query = SourceQuery('src_query')

    # empty query
    instr_query = InstrumentQuery(name='empty_instrument_query',
                                  input_prod_list_name='p_list',
                                  catalog=None,
                                  catalog_name='user_catalog')

    empty_query = EmptyProductQuery('empty_parameters_dummy_query',)
    failing_query = FailingProductQuery('failing_parameters_dummy_query',)
    # let's build a simple parameter to its list
    p = BoundaryFloat(value=10., name='p', units='W',)
    numerical_query = DataServerNumericQuery('numerical_parameters_dummy_query',
                                             parameters_list=[p])

    f = FileReference(downloadable=True, name='dummy_file')
    file_query = FileParameterQuery('file_parameters_dummy_query',
                                    parameters_list=[p, f])

    # let's build a simple parameter to its list
    sb = SpectralBoundary(value=10., name='sb')
    parametrical_query = DataServerParametricQuery('parametrical_parameters_dummy_query',
                                                   parameters_list=[sb])

    ang = Angle(value=1., units='arcsec', default_units='arcsec', name='ang')
    ang_deg = Angle(value=1., units='deg', default_units='arcsec', name='ang_deg')
    energ = Energy(value=1., E_units='MeV', name='energ', units_name='energy_units')
    echo_param_query = EchoProductQuery('echo_parameters_dummy_query',
                                        parameters_list=[ang, ang_deg, energ])
    
    bounded_int_param = Integer(5, name = 'bounded_int_par', min_value = 2, max_value = 8)
    bounded_float_param = Float(5., name = 'bounded_float_par', min_value = 2.2, max_value = 7.7)
    string_select_param = String('spam', name='string_select_par', allowed_values=('spam', 'eggs', 'ham'))
    restricted_param_query = EchoProductQuery('restricted_parameters_dummy_query',
                                                    parameters_list=[bounded_int_param, 
                                                                     bounded_float_param,
                                                                     string_select_param])

    struct_par = StructuredParameter({'a': [1, 2]}, name='struct')
    structured_echo_query = StructuredEchoProductQuery('structured_param_dummy_query', parameters_list=[struct_par])
    # this dicts binds the product query name to the product name from frontend
    # eg my_instr_image is the parameter passed by the fronted to access the
    # the MyInstrMosaicQuery, and the dictionary will bind
    # query_dictionary['my_instr_image'] = 'my_instr_image_query'
    query_dictionary = {}
    # the empty instrument does not effectively do anything and therefore support any particular query
    # nor product, only a simple query that does not return anything
    query_dictionary['dummy'] = 'empty_parameters_dummy_query'
    query_dictionary['numerical'] = 'numerical_parameters_dummy_query'
    query_dictionary['file_dummy'] = 'file_parameters_dummy_query'
    query_dictionary['failing'] = 'failing_parameters_dummy_query'
    query_dictionary['parametrical'] = 'parametrical_parameters_dummy_query'
    query_dictionary['echo'] = 'echo_parameters_dummy_query'
    query_dictionary['restricted'] = 'restricted_parameters_dummy_query'
    query_dictionary['structured'] = 'structured_param_dummy_query'

    return Instrument('empty',
                      src_query=src_query,
                      instrumet_query=instr_query,
                      product_queries_list=[empty_query, 
                                            numerical_query,
                                            file_query,
                                            failing_query, 
                                            parametrical_query, 
                                            echo_param_query, 
                                            restricted_param_query,
                                            structured_echo_query],
                      query_dictionary=query_dictionary)
