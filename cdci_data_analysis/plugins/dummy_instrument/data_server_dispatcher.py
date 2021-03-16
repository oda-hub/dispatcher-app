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

from cdci_data_analysis.analysis.queries import ProductQuery
from cdci_data_analysis.analysis.products import QueryOutput

class AysnchExcept(Exception):
    pass

class DataServerQuery(ProductQuery):

    def __init__(self, name):
        super(DataServerQuery, self).__init__(name)

    def test_connection(self):
        pass

    def test_has_input_products(self):
        pass

    def get_dummy_products(self, instrument, config=None, **kwargs):
        return []

    def process_product_method(self, instrument, prod_list,api=False, **kw):
        query_out = QueryOutput()
        return query_out