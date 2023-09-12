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

import json
import os

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

from oda_api.data_products import ApiCatalog
from raven.utils.urlparse import urlparse

from cdci_data_analysis.analysis.catalog import BasicCatalog
from cdci_data_analysis.analysis.queries import ProductQuery
from cdci_data_analysis.analysis.products import BaseQueryProduct, QueryOutput, QueryProductList, ImageProduct
from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.exceptions import InternalError

from oda_api.data_products import NumpyDataProduct, NumpyDataUnit

import numpy as np

import logging

logger = logging.getLogger(__name__)


class AsynchExcept(Exception):
    pass


class DataServerQuery:
    def __init__(self, config=None, instrument=None):
        pass

    def test_communication(self,
                           instrument: Instrument=None,
                           query_type='Real',
                           logger=None,
                           config=None,
                           sentry_dsn=None) -> QueryOutput:
        query_out = QueryOutput()
        query_out.set_done(message="mock ok message!", debug_message="mock ok debug_message")
        return query_out

    def test_has_input_products(self, instrument: Instrument, logger) -> Tuple[QueryOutput, list]:
        query_out = QueryOutput()
        query_out.set_done(message="mock ok message!", debug_message="mock ok debug_message")
        return query_out, []
    
    status_fn = "DataServerQuery-status.state"

    @classmethod
    def set_status(cls, status):
        open(cls.status_fn, "w").write(status)

    @classmethod
    def get_status(cls):
        if os.path.exists(cls.status_fn):
            return open(cls.status_fn).read()
        else:
            return None

    def decide_status(self):
        # callback will be sent separately, so we can detect a marker here
        if os.path.exists(self.status_fn):
            return open(self.status_fn).read().strip()
        else:
            return "submitted"


    def run_query(self, *args, **kwargs):
        logger.warn('fake run_query in %s with %s, %s', self, args, kwargs)
        
        query_out = QueryOutput()
        
        status = self.decide_status() 
        if status == "submitted":
            query_out.set_done(message="job submitted mock",
                            debug_message="no message really",
                            job_status='submitted',
                            comment="mock comment",
                            warning="mock warning")
        if status == "progress":
            query_out.set_done(message="job progress mock",
                               debug_message="no message really",
                               job_status='progress',
                               comment="mock comment",
                               warning="mock warning")
        elif status == "done":
            query_out.set_done(message="job done mock",
                            debug_message="no message really",
                            job_status='done',
                            comment="mock comment",
                            warning="mock warning")
        elif status == "failed":
            query_out.set_failed(message="job failed mock",
                            debug_message="no message really",
                            job_status='failed')
        else:
            NotImplementedError


        return None, query_out


class DataServerLogSubmitQuery(DataServerQuery):

    def run_query(self, *args, **kwargs):
        logger.warn('fake run_query in %s with %s, %s', self, args, kwargs)

        query_out = QueryOutput()

        current_status = self.get_status()

        if current_status == '':
            # request sent to the backend
            self.set_status("submitted")
            query_out.set_done(message="job submitted mock",
                               debug_message="no message really",
                               job_status="submitted",
                               comment="mock comment",
                               warning="mock warning")

        elif current_status == "submitted":
            query_out.set_done(message="job submitted mock",
                               debug_message="no message really",
                               job_status='submitted',
                               comment="mock comment",
                               warning="mock warning")

        elif current_status == "done":
            query_out.set_done(message="job done mock",
                               debug_message="no message really",
                               job_status='done',
                               comment="mock comment",
                               warning="mock warning")


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
        
        try:
            query_out.prod_dictionary['input_param_scw_list'] = prod_list.prod_list[0].data
        except Exception as e:
            logger.info("unable to set input_param_scw_list - this is fine")


        query_out.prod_dictionary['prod_process_message'] = ''

        return query_out

    def build_product_list(self, instrument, res, out_dir, prod_prefix='', api=False):
        #TODO: return here the parameters
        p = BaseQueryProduct('input_param_scw_list', 
                              data=NumpyDataProduct(NumpyDataUnit(
                                                      data=np.array([]), 
                                                      meta_data={'scw_list': self.input_param_scw_list})))
        return [p]

    def test_communication(self,
                           instrument: Instrument,
                           query_type='Real',
                           logger=None,
                           config=None,
                           sentry_dsn=None) -> QueryOutput:
        query_out = QueryOutput()
        query_out.set_done(message="mock ok message!", debug_message="mock ok debug_message")
        return query_out

    def test_has_input_products(self, instrument: Instrument, logger) -> Tuple[QueryOutput, list]:
        query_out = QueryOutput()
        query_out.set_done(message="mock ok message!", debug_message="mock ok debug_message")
        return query_out, []

    def get_data_server_query(self, instrument: Instrument, config=None):
        if instrument.data_server_query_class:
            q = instrument.data_server_query_class(instrument=instrument, config=config)
        else:
            q = DataServerQuery()
        self.input_param_scw_list = instrument.get_par_by_name('scw_list').value
        return q

    # example with the general user role
    def check_query_roles(self, roles, par_dic):
        # if use_max_pointings > 50 or scw_list.split(",") > 50:
        #     return 'unige-hpc-full' in roles:
        #
        # return True
        results = dict(authorization=True, needed_roles=[])
        return results


class EmptyLogSubmitProductQuery(EmptyProductQuery):

    def get_data_server_query(self, instrument: Instrument, config=None):
        q = DataServerLogSubmitQuery()
        self.input_param_scw_list = instrument.get_par_by_name('scw_list').value
        return q


class FailingProductQuery(EmptyProductQuery):

    def __init__(self, name='unset-name', config=None, instrument=None):
        super().__init__(name)

    def get_dummy_products(self, instrument, config=None, **kwargs):
        raise InternalError("failing query")


class DataServerNumericQuery(ProductQuery):

    def __init__(self, name, parameters_list=[],):
        super(DataServerNumericQuery, self).__init__(name, parameters_list=parameters_list)

    def test_connection(self):
        pass

    def test_has_input_products(self):
        pass

    def get_dummy_products(self, instrument, config=None, **kwargs):
        user_catalog = None
        if len(instrument.instrumet_query.parameters) > 0:
            for par in instrument.instrumet_query.parameters:
                if par.name == 'user_catalog' and par.value is not None:
                    user_catalog = instrument.instrumet_query.parameters[0]

        # create dummy NumpyDataProduct
        meta_data = {'product': 'mosaic', 'instrument': 'empty', 'src_name': '',
                     'query_parameters': self.get_parameters_list_as_json()}

        ima = NumpyDataUnit(np.zeros((100, 100)), hdu_type='image')
        data = NumpyDataProduct(data_unit=ima)
        # build image product
        image = ImageProduct(name='user_image',
                             data=data,
                             file_dir=None,
                             file_name=None,
                             meta_data=meta_data)

        prod_list = QueryProductList(prod_list=[image])
        if user_catalog is not None and user_catalog.value is not None:
            prod_list.prod_list.append(user_catalog)

        return prod_list

    def build_product_list(self, instrument, res, out_dir, prod_prefix='', api=False):
        return []

    def process_product_method(self, instrument, prod_list, api=False, **kw):
        query_out = QueryOutput()
        if len(prod_list.prod_list) > 0:
            for prod in prod_list.prod_list:
                if hasattr(prod, 'name'):
                    if prod.name == 'user_catalog':
                        # catalog
                        query_out.prod_dictionary['catalog'] = prod.value.get_dictionary()
                    if prod.name == 'user_image':
                        # image
                        query_out.prod_dictionary['numpy_data_product_list'] = [prod.data]

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
                results['needed_roles_with_comments'] = {
                    'general': 'general role is needed for p>50',
                    'unige-hpc-full': 'unige-hpc-full role is needed for p>50 as well'
                }
        return results


class DataServerParametricQuery(ProductQuery):

    def __init__(self, name, parameters_list=None):
        if parameters_list is None:
            parameters_list = []
        super().__init__(name, parameters_list=parameters_list)

    def test_connection(self):
        pass

    def get_dummy_products(self, instrument, config=None, **kwargs):
        return []

    def test_has_input_products(self):
        pass

    def build_product_list(self, instrument, res, out_dir, prod_prefix='', api=False):
        return []

    def process_product_method(self, instrument, prod_list, api=False, **kw):
        # TODO if needed, some products should be build here
        query_out = QueryOutput()

        return query_out

    def get_data_server_query(self, instrument: Instrument, config=None):
        if instrument.data_server_query_class:
            return instrument.data_server_query_class(instrument=instrument, config=config)
        return DataServerQuery()

    # example with the general user role
    def check_query_roles(self, roles, par_dic):
        results = dict(authorization=True, needed_roles=[])
        return results

class EchoProductQuery(ProductQuery):
    def __init__(self, name, parameters_list=None):
        if parameters_list is None:
            parameters_list = []
        super().__init__(name, parameters_list=parameters_list)
     
    def get_data_server_query(self, instrument, config=None, **kwargs):
        param_names = instrument.get_parameters_name_list()
        # this is very special test plugin to echo values as-is. 
        # In real plugins .get_default_value() should be passed to backend
        param_dict = {x: instrument.get_par_by_name(x).value for x in param_names}
        return EchoServerDispatcher(instrument=instrument, param_dict=param_dict)
    
    def build_product_list(self, instrument, res, out_dir, api=False):
        return [res]
    
    def process_product_method(self, instrument, prod_list, api=False, **kw):
        query_out = QueryOutput()
        query_out.prod_dictionary['echo'] = prod_list.prod_list[0]
        return query_out
    
    def test_communication(self, instrument, query_type='Real', logger=None, config=None, sentry_dsn=None):
        query_out = QueryOutput()
        query_out.set_done()
        return query_out
    
    def test_has_products(self, instrument, query_type='Real', logger=None, config=None, scratch_dir=None, sentry_dsn=None):
        query_out = QueryOutput()
        query_out.prod_dictionary['input_prod_list'] = []
        query_out.set_done()
        return query_out
        

class EchoServerDispatcher:
    def __init__(self, instrument=None, param_dict=None):
        self.param_dict = param_dict
    
    def run_query(self, *args, **kwargs):
        query_out = QueryOutput()
        query_out.set_done(job_status='done')
        res = self.param_dict
        return res, query_out