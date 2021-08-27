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

import string
import json
import logging
import yaml

import numpy as np
from astropy.table import Table

from cdci_data_analysis.analysis.queries import _check_is_base_query
from ..analysis import tokenHelper
from .catalog import BasicCatalog
from .products import QueryOutput
from .queries import ProductQuery, SourceQuery, InstrumentQuery
from .io_helper import upload_file
from .exceptions import RequestNotUnderstood, RequestNotAuthorized

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f

class DataServerQueryClassNotSet(Exception):
    pass

class Instrument:
    def __init__(self,
                 instr_name,
                 src_query,
                 instrumet_query,
                 input_product_query=None,
                 asynch=True,
                 catalog=None,
                 data_serve_conf_file=None,
                 product_queries_list=None,
                 data_server_query_class=None,
                 query_dictionary={}):

        # name
        self.name = instr_name
        # logger
        self.logger = logging.getLogger(repr(self))
        #src query
        self.src_query=src_query
        # asynch
        self.asynch=asynch
        #Instrument specific
        self.instrumet_query=instrumet_query
        #self.data_serve_conf_file=data_serve_conf_file
        self.set_data_server_conf_dict(data_serve_conf_file)
        self.product_queries_list=product_queries_list
        self._queries_list=[self.src_query,self.instrumet_query]
        self.data_server_query_class = data_server_query_class

        if product_queries_list is not None and product_queries_list !=[]:
            self._queries_list.extend(product_queries_list)

        _check_is_base_query(self._queries_list)

        self.input_product_query=input_product_query

        self.query_dictionary = query_dictionary

    def __repr__(self):
        return f"[ {self.__class__.__name__} : {self.name} ]"

    def set_data_server_conf_dict(self,data_serve_conf_file):
        conf_dict=None
        #print ('--> setting set_data_server_conf_dict for', self.name,'from data_serve_conf_file',data_serve_conf_file)
        if data_serve_conf_file is not None:
           with open(data_serve_conf_file, 'r') as ymlfile:
                cfg_dict = yaml.load(ymlfile, Loader=yaml.SafeLoader)
                for k in cfg_dict['instruments'].keys():
                    #print ('name',k)
                    if self.name ==k:
                        #print('name', k,cfg_dict['instruments'][k])
                        conf_dict=cfg_dict['instruments'][k]

        self.data_server_conf_dict=conf_dict

    def _check_names(self):
        pass


    def set_pars_from_dic(self,par_dic,verbose=False):
        for _query in self._queries_list:
            for par in _query._parameters_list:
                par.set_from_form(par_dic,verbose=verbose)

    def set_par(self,par_name,value):
        p=self.get_par_by_name(par_name)
        p.value=value

    def get_query_by_name(self, prod_name):
        p=None
        for _query in self._queries_list:
            if prod_name == _query.name:
                p = _query

        if p is None:
            raise Warning('query', prod_name, 'not found')

        return p

    def test_communication(self,config,logger=None):
        if self.data_server_query_class is not None:
            return self.data_server_query_class(config=config, instrument=self).test_communication(logger=logger)
        else:
            raise DataServerQueryClassNotSet('in test_communication')

    def test_busy(self, config,logger=None):
        if self.data_server_query_class is not None:
            return self.data_server_query_class(config=config).test_busy(logger=logger)

    def test_has_input_products(self, config, instrument,logger=None):
        if self.data_server_query_class is not None:
            return self.data_server_query_class(config=config,instrument=self).test_has_input_products(instrument,logger=logger)

    def parse_inputs_files(self,
                             par_dic,
                             request,
                             temp_dir,
                             verbose,
                             use_scws,
                             sentry_client=None):
        # TODO probably exception handling can be further improved and/or optmized
        # set catalog
        try:
            self.upload_catalog_from_fronted(par_dic=par_dic, request=request, temp_dir=temp_dir)
        except Exception as e:
            if sentry_client is not None:
                sentry_client.capture('raven.events.Message',
                                           message=f'Error while uploading catalog file from the frontend {e}')
            raise RequestNotUnderstood("Error while uploading catalog file from the frontend")
        try:
            self.set_catalog(par_dic)
        except Exception as e:
            if sentry_client is not None:
                sentry_client.capture('raven.events.Message',
                                           message=f'Error while setting catalog file from the frontend {e}')
            raise RequestNotUnderstood("Error while setting catalog file from the frontend")
        try:
            input_file_path = self.upload_input_products_from_fronted(request=request, temp_dir=temp_dir)
        except Exception as e:
            if sentry_client is not None:
                sentry_client.capture('raven.events.Message',
                                           message=f'Error while uploading scw_list file from the frontend {e}')
            raise RequestNotUnderstood("Error while uploading scw_list file from the frontend")

        if input_file_path is None and use_scws == 'user_file':
            raise RequestNotUnderstood(
                "scw_list file was expected to be passed, but it has not been found, "
                "please check the inputs")
        elif input_file_path is not None and use_scws != 'user_file':
            raise RequestNotUnderstood("scw_list file was found "
                                       "despite use_scws was indicating this was not provided,"
                                       " please check the inputs")
        try:
            self.set_input_products_from_fronted(input_file_path=input_file_path, par_dic=par_dic, verbose=verbose)
        except Exception as e:
            if sentry_client is not None:
                sentry_client.capture('raven.events.Message',
                                           message=f'Error while setting input scw_list file from the frontend {e}')
            raise RequestNotUnderstood("Error while setting input scw_list from the frontend")
        self.set_pars_from_dic(par_dic, verbose=verbose)

    def run_query(self, product_type,
                  par_dic,
                  request,
                  back_end_query,
                  job,
                  run_asynch,
                  config=None,
                  out_dir=None,
                  query_type='Real',
                  verbose=False,
                  logger=None,
                  sentry_client=None,
                  dry_run=False,
                  api=False,
                  decoded_token=None,
                  **kwargs):

        if logger is None:
            logger = self.get_logger()

        #  this was removed by 2f5b5dfb7e but turns out it is used by some plugins, see test_server_plugin_integral_all_sky
        self._current_par_dic=par_dic

        # set pars values from the input parameters
        query_out = self.set_pars_from_form(par_dic, verbose=verbose, sentry_client=sentry_client)
        if verbose:
            self.show_parameters_list()

        logger.info('--> par dict', par_dic)

        if dry_run:
            job.set_done()
            if query_out.status_dictionary['status'] == 0:
                query_out.set_done(message='dry-run',job_status=job.status)
                query_out.set_instrument_parameters(self.get_parameters_list_as_json(prod_name=product_type))
        else:
            if query_out.status_dictionary['status'] == 0:
                query_out = QueryOutput()
                message = ''
                debug_message = ''
                try:
                    query_name = self.get_product_query_name(product_type)
                    query_obj = self.get_query_by_name(query_name)
                    roles = []
                    if decoded_token is not None: # otherwise the request is public
                        roles = tokenHelper.get_token_roles(decoded_token)
                    # assess the permissions for the query execution
                    self.check_instrument_query_role(query_obj, product_type, roles, par_dic)

                    query_obj = self.get_query_by_name(query_name)
                    query_out = query_obj.run_query(self, out_dir, job, run_asynch,
                                                    query_type=query_type,
                                                    config=config,
                                                    logger=logger,
                                                    sentry_client=sentry_client,
                                                    api=api)
                    if query_out.status_dictionary['status'] == 0:
                        if 'comment' in query_out.status_dictionary.keys():
                            backend_comment = query_out.status_dictionary['comment']
                        else:
                            backend_comment = ''
                        if 'warning' in query_out.status_dictionary.keys():
                            backend_warning = query_out.status_dictionary['warning']
                        else:
                            backend_warning = ''

                        query_out.set_done(message = message,
                                           debug_message=str(debug_message),
                                           comment=backend_comment,
                                           warning=backend_warning)
                    else:
                        pass
                except RequestNotAuthorized:
                    raise
                except RequestNotUnderstood as e:
                    logger.warning("bad request from user, passing through: %s", e)
                    raise
                except Exception as e: # we shall not do that
                    logger.error("run_query failed: %s", e)
                    # logger.error("run_query failed: %s", traceback.format_exc())
                    query_out.set_failed(product_type, logger=logger, sentry_client=sentry_client, excep=e)

        # adding query parameters to final products
        query_out.set_analysis_parameters(par_dic)
        # TODO perhaps this will change
        query_out.set_api_code(par_dic, url=back_end_query.config.products_url + "/dispatch-data")
        query_out.dump_analysis_parameters(out_dir, par_dic)

        return query_out

    def get_product_query_name(self, product_type):
        if product_type not in self.query_dictionary:
            raise Exception(f"product type {product_type} not in query_dictionary {self.query_dictionary}")
        else:
            return self.query_dictionary[product_type]

    def check_instrument_query_role(self, query_obj, product_type, roles, par_dic):
        results = query_obj.check_query_roles(roles, par_dic)
        if not results['authorization']:
            results['needed_roles']

            lacking_roles = sorted(list(set(results['needed_roles']) - set(roles)))

            lacking_roles_comment = "\n".join([
                f" - {role}: {results.get('needed_roles_with_comments', {}).get(role, 'please refer to support for details')}"
                for role in lacking_roles
            ])

            raise RequestNotAuthorized(
                f"Unfortunately, your priviledges are not sufficient to make the request for this particular product and parameter combination.\n"
                f"- Your priviledge roles include {roles}\n"
                f"- You are lacking all of the following roles:\n"
                f"{lacking_roles_comment}\n"
                f"You can request support if you think you should be able to make this request."
            )
        else:
            return True

    def get_html_draw(self, prod_name, image,image_header,catalog=None,**kwargs):
        return self.get_query_by_name(prod_name).get_html_draw( image,image_header,catalog=catalog,**kwargs)

    #def get_par_by_name(self,par_name, validate=False):
    def get_par_by_name(self,par_name):
        p=None

        for _query in self._queries_list:
            if par_name in _query.par_names:
                p  =  _query.get_par_by_name(par_name)

        if p is None:
            raise Warning('parameter', par_name, 'not found')

     #   if validate and hasattr(p, 'check_value'):
     #       p.check_value(p.value)

        return p

    def show_parameters_list(self):

        print ("-------------")
        for _query in self._queries_list:
            print ('q:',_query.name)
            _query.show_parameters_list()
        print("-------------")

    def get_parameters_list_as_json(self,add_src_query=True,add_instr_query=True,prod_name=None):

        l=[{'instrumet':self.name}]
        l.append({'prod_dict':self.query_dictionary})
        #print('--> dict',self.query_dictionary)


        for _query in self._queries_list:
            _add_query = True
            if isinstance(_query,SourceQuery) and add_src_query==False:
                _add_query=False
                #print('src',_query.name)

            if isinstance(_query,InstrumentQuery) and add_instr_query==False:
                _add_query=False
            #print('isntr', _query.name)

            if isinstance(_query, ProductQuery) and prod_name is not None and _query.name==self.query_dictionary[prod_name]:
                _add_query = True
                #print('prd', _query.name,prod_name)
            elif isinstance(_query, ProductQuery) and prod_name is not None and _query.name!=self.query_dictionary[prod_name]:
                #print('prd', _query.name, prod_name)
                _add_query = False

            if _add_query == True:
                l.append(_query.get_parameters_list_as_json(prod_dict=self.query_dictionary))

        return l

    def get_parameters_name_list(self, prod_name=None):
        l = []
        _add_query = False
        for _query in self._queries_list:
            _add_query = True
            print('q:', _query.name)

            if isinstance(_query, SourceQuery):
                _add_query = True

            if isinstance(_query, InstrumentQuery):
                _add_query = True
            # print('isntr', _query.name)

            if isinstance(_query, ProductQuery) and prod_name is not None and _query.name == self.query_dictionary[
                prod_name]:
                _add_query = True
                # print('prd', _query.name,prod_name)
            elif isinstance(_query, ProductQuery) and prod_name is not None and _query.name != self.query_dictionary[
                prod_name]:
                # print('prd', _query.name, prod_name)
                _add_query = False

            if _add_query == True:
                for _par in _query._parameters_list:
                    l.append(_par.name)

        return l

    def set_pars_from_form(self,par_dic,logger=None,verbose=False,sentry_client=None):
        #print('---------------------------------------------')
        #print('setting form paramters')
        q=QueryOutput()
        #status=0
        error_message=''
        debug_message=''
        if logger is None:
            logger = logging.getLogger(__name__)

        try:
            self.set_pars_from_dic(par_dic,verbose=verbose)
            #DONE
            q.set_done(debug_message=str(debug_message))
        except RequestNotUnderstood as e:
           q.set_failed(f"please adjust request parameters: {e.message}",
                        logger=logger,
                        sentry_client=None,
                        excep=e)

        except Exception as e:
            #FAILED
            m = f'problem setting form parameters from dict: {par_dic}'
            logger.error(m)

            q.set_failed(m,
                         logger=logger,
                         sentry_client=sentry_client,
                         excep=e)
            #status=1
            #error_message= 'error in form parameter'
            #debug_message = e
            #logger.exception(e)
        #print('---------------------------------------------')
        return q

    def upload_input_products_from_fronted(self, request, temp_dir):
        input_file_path = None
        if request.method == 'POST':
            # save to a temporary folder, and delete it afterwards
            input_file_path = upload_file('user_scw_list_file', temp_dir)
        return input_file_path

    def set_input_products_from_fronted(self, input_file_path, par_dic, verbose=False):
        input_prod_list_name = self.instrumet_query.input_prod_list_name
        if input_file_path is not None:
            has_input = self.set_input_products(par_dic, input_file_path, input_prod_list_name)
            if has_input:
                pass
            else:
                raise RuntimeError

    def set_input_products(self, par_dic, input_file_path,input_prod_list_name):
        if input_file_path is None:
            #if no file we pass OK condition
            #since the paramter will be passed from the form
            has_prods=True
        else:
            try:
                with open(input_file_path) as f:
                    _lines = f.readlines()
                    lines = []
                    for ll in _lines:
                         lines.extend(ll.split(","))
                    lines = [item.strip() for item in lines]
                    cleaned_lines=[]
                    for line in lines:
                        line = ''.join([x if x in string.printable else '' for x in line])
                        cleaned_lines.append(line)

                par_dic[input_prod_list_name] = cleaned_lines
                has_prods= len(lines) >= 1
            except:
                has_prods=False

        return has_prods

    def upload_catalog_from_fronted(self, par_dic, request, temp_dir):
        if request.method == 'POST':
            # save to a temporary folder, and delete it afterwards
            cat_file_path = upload_file('user_catalog_file', temp_dir)
            if cat_file_path is not None:
                par_dic['user_catalog_file'] = cat_file_path

    def set_catalog(self, par_dic):
        user_catalog_file = None
        if 'user_catalog_file' in par_dic.keys():
            user_catalog_file = par_dic['user_catalog_file']

        if 'user_catalog_dictionary' in par_dic.keys() and par_dic['user_catalog_dictionary'] is not None:
            if type(par_dic['user_catalog_dictionary']) == dict:
                self.set_par('user_catalog', build_catalog(par_dic['user_catalog_dictionary']))
            else:
                catalog_dic = json.loads(par_dic['selected_catalog'])
                self.set_par('user_catalog', build_catalog(catalog_dic))

        if user_catalog_file is not None:
            self.set_par('user_catalog', load_user_catalog(user_catalog_file))
        else:
            if 'catalog_selected_objects' in par_dic.keys():
                catalog_selected_objects = np.array(par_dic['catalog_selected_objects'].split(','), dtype=np.int)
            else:
                catalog_selected_objects = None
            if 'selected_catalog' in par_dic.keys():
                catalog_dic = json.loads(par_dic['selected_catalog'])
                user_catalog = build_catalog(catalog_dic, catalog_selected_objects)
                self.set_par('user_catalog', user_catalog)


def load_user_catalog(user_catalog_file):
    return BasicCatalog.from_file(user_catalog_file)


def build_catalog(cat_dic,catalog_selected_objects=None):
    from astropy import units as u
    from astropy.coordinates import Angle, Latitude, Longitude
    t = Table(cat_dic['cat_column_list'], names=cat_dic['cat_column_names'])
    src_names = t['src_names']
    significance = t['significance']
    lon =Longitude(t[cat_dic['cat_lon_name']],unit=u.deg)
    lat = Latitude(t[cat_dic['cat_lat_name']],unit=u.deg)

    frame = cat_dic['cat_frame']
    unit =cat_dic['cat_coord_units']
    #print (unit,lon,lat)

    user_catalog =BasicCatalog(src_names, lon, lat, significance, _table=t, unit=unit, frame=frame)


    meta_ids = user_catalog._table['meta_ID']
    IDs=[]
    for ID,cat_ID in enumerate(meta_ids):
        if catalog_selected_objects is not None:
            if cat_ID in catalog_selected_objects:
                IDs.append(ID)
        else:
            IDs.append(ID)

    #TODO: check this indentation

    print('selected IDs',IDs)
    user_catalog.select_IDs(IDs)

    return user_catalog
