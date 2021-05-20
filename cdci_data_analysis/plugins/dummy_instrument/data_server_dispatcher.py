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

import urllib
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

from typing import Tuple

from raven.utils.urlparse import urlparse

from cdci_data_analysis.analysis.queries import ProductQuery
from cdci_data_analysis.analysis.products import QueryOutput
from cdci_data_analysis.analysis.instrument import Instrument

import logging

logger = logging.getLogger(__name__)


class AsynchExcept(Exception):
    pass


class DataServerQuery:
    def __init__(self, config=None, instrument=None):
        pass

    def test_communication(self,
                           instrument: Instrument,
                           query_type='Real',
                           logger=None,
                           config=None,
                           sentry_client=None) -> QueryOutput:
        query_out = QueryOutput()
        query_out.set_done(message="mock ok message!", debug_message="mock ok debug_message")
        return query_out

    def test_has_input_products(self, instrument: Instrument, logger) -> Tuple[QueryOutput, list]:
        query_out = QueryOutput()
        query_out.set_done(message="mock ok message!", debug_message="mock ok debug_message")
        return query_out, []

    def run_query(self, *args, **kwargs):
        logger.warn('fake run_query in %s with %s, %s', self, args, kwargs)

        query_out = QueryOutput()
        query_out.set_done(message="job submitted mock",
                           debug_message="no message really",
                           job_status='submitted',
                           comment="mock comment",
                           warning="mock warning")

        # TODO: track somehow status of our mock job and return done after several requests
        # otherwise not it never returns submitted

        return None, query_out


class DataServerQuerySemiAsync(DataServerQuery):
    def __init__(self, config=None, instrument=None):
        self.instrument = instrument
        super(DataServerQuerySemiAsync, self).__init__()

    def run_query(self, *args, **kwargs):
        logger.warn('fake run_query in %s with %s, %s', self, args, kwargs)
        query_out = QueryOutput()

        p_value = self.instrument.get_par_by_name('p').value

        if p_value == -1:
            query_out.set_done(message="job failed mock",
                               debug_message="no message really",
                               job_status='failed',
                               comment="mock comment",
                               warning="mock warning")
        elif 0 <= p_value < 4:
            query_out.set_done(message="job submitted mock",
                               debug_message="no message really",
                               job_status='submitted',
                               comment="mock comment",
                               warning="mock warning")
        else:
            query_out.set_done(message="job done mock",
                               debug_message="no message really",
                               job_status='done',
                               comment="mock comment",
                               warning="mock warning")

        return None, query_out


class EmptyProductQuery(ProductQuery):

    def __init__(self, name='unset-name', config=None, instrument=None):
        super().__init__(name)

    def test_connection(self):
        pass

    def get_dummy_products(self, instrument, config=None, **kwargs):
        return []

    def process_product_method(self, instrument, prod_list,api=False, **kw):
        query_out = QueryOutput()
        return query_out

    def build_product_list(self, instrument, res, out_dir, prod_prefix='', api=False):
        return []

    def test_communication(self,
                           instrument: Instrument,
                           query_type='Real',
                           logger=None,
                           config=None,
                           sentry_client=None) -> QueryOutput:
        query_out = QueryOutput()
        query_out.set_done(message="mock ok message!", debug_message="mock ok debug_message")
        return query_out

    def test_has_input_products(self, instrument: Instrument, logger) -> Tuple[QueryOutput, list]:
        query_out = QueryOutput()
        query_out.set_done(message="mock ok message!", debug_message="mock ok debug_message")
        return query_out, []

    def get_data_server_query(self, instrument: Instrument, config=None):
        if instrument.data_server_query_class:
            return instrument.data_server_query_class(instrument=instrument, config=config)
        return DataServerQuery()

    # example with the general user role
    def check_query_roles(self, roles, par_dic):
        # if use_max_pointings > 50 or scw_list.split(",") > 50:
        #     return 'unige-hpc-full' in roles:
        #
        # return True
        results = dict(authorization=True, needed_roles=[])
        return results


class DataServerNumericQuery(ProductQuery):

    def __init__(self, name, parameters_list=[],):
        super(DataServerNumericQuery, self).__init__(name, parameters_list=parameters_list)

    def test_connection(self):
        pass

    def test_has_input_products(self):
        pass

    def get_dummy_products(self, instrument, config=None, **kwargs):
        return []

    def build_product_list(self, instrument, res, out_dir, prod_prefix='', api=False):
        return []

    def process_product_method(self, instrument, prod_list, api=False, **kw):
        query_out = QueryOutput()
        return query_out

    def get_data_server_query(self, instrument: Instrument, config=None):
        if instrument.data_server_query_class:
            return instrument.data_server_query_class(instrument=instrument, config=config)
        return DataServerQuery()

    # example with the general user role
    def check_query_roles(self, roles, par_dic):
        param_p = self.get_par_by_name('p')
        results = dict(authorization='general' in roles, needed_roles=['general'])
        if 'p' in par_dic.keys():
            # not sure this is actually the best way to obtain a certain parameter value
            # p = float(par_dic['p'])
            # better now, it extracts the value directly from the related parameter object
            p = param_p.value
            if p > 50:
                results['authorization'] = 'general' and 'unige-hpc-full' in roles
                results['needed_roles'] = ['general', 'unige-hpc-full']
        return results