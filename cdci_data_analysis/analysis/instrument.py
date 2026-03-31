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

import os
from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

import string
import json
import logging
import yaml
import validators

import numpy as np
from astropy.table import Table
from urllib.parse import urlencode

from cdci_data_analysis.analysis.queries import _check_is_base_query
from .parameters import POSIXPath
from ..analysis import tokenHelper, parameters
from .catalog import BasicCatalog
from .products import QueryOutput
from .queries import ProductQuery, SourceQuery, InstrumentQuery
from .io_helper import upload_file, upload_files_request

from .exceptions import RequestNotUnderstood, RequestNotAuthorized, InternalError, ProductProcessingError
from ..flask_app.sentry import sentry

from oda_api.api import DispatcherAPI, RemoteException, Unauthorized, DispatcherException, DispatcherNotAvailable, UnexpectedDispatcherStatusCode, RequestNotUnderstood as RequestNotUnderstoodOdaApi

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f

# list of parameters not to be included in the par_dic object

# TODO: this is not preserved between requests, and is not thread safe. Why not pass it in class instances?
params_not_to_be_included = ['user_catalog',]

non_parameter_args = ['instrument', 
                      'query_status', 
                      'query_type', 
                      'product_type', 
                      'session_id', 
                      'token',
                      'api',
                      'oda_api_version',
                      'off_line',
                      'job_id',
                      'async_dispatcher',
                      'allow_unknown_args',
                      'catalog_selected_objects',
                      'run_asynch']
# NOTE: arguments are passed in the request to the dispatcher
# some arguments are used to set the values of the analysis parameters
# the parameter is a subclass of Parameter and may use several arguments to set it's value
# some arguments may be used to set values of several parameters 
# (e.g. for Time(name='T1') and Time(name='T2') arguments will be T1, T2, T_format)

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
                 query_dictionary={},
                 allow_unknown_arguments=False):

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
        
        self.allow_unknown_arguments = allow_unknown_arguments

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

    def set_pars_from_dic(self, arg_dic, verbose=False):
        product_type = arg_dic.get('product_type', None)
        if product_type is not None:
            query_name = self.get_product_query_name(product_type)
            query_obj = self.get_query_by_name(query_name)
            # loop over the list of parameters for the requested query,
            # but also of the instrument query and source query
            param_list = (query_obj.parameters +
                          self.instrumet_query.parameters +
                          self.src_query.parameters)
        else:
            param_list = [par for _query in self._queries_list for par in _query.parameters]

        updated_arg_dic = arg_dic.copy()
        
        for par in param_list:
            self.logger.info("before normalizing, set_pars_from_dic>> par: %s par.name: %s par.value: %s par_dic[par.name]: %s",
                             par, par.name, par.value, arg_dic.get(par.name, None))
            # this is required because in some cases a parameter is set without a name (eg UserCatalog),
            # or they don't have to set (eg scw_list)
            if par.name is not None and par.name not in params_not_to_be_included:
                if isinstance(par, POSIXPath) and par.name + '_type' in arg_dic and arg_dic[par.name + '_type'] == 'file'\
                        and par.name not in arg_dic:
                    par.value = None

                # set the value for par to a default format,
                # or to a default value if this is not included within the request
                updated_arg_dic[par.name] = par.set_value_from_form(arg_dic, verbose=verbose)
                if par.units_name is not None:
                    if par.default_units is not None:
                        updated_arg_dic[par.units_name] = par.default_units
                    else:
                        raise InternalError("Error when setting the parameter %s: "
                                            "default unit not specified" % par.name)
                if par.par_format_name is not None:
                    if par.par_default_format is not None:
                        updated_arg_dic[par.par_format_name] = par.par_default_format
                    else:
                        raise InternalError("Error when setting the parameter %s: "
                                            "default format not specified" % par.name)

            self.logger.info("after normalizing, set_pars_from_dic>> par: %s par.name: %s par.value: %s par_dic[par.name]: %s",
                             par, par.name, par.value, arg_dic.get(par.name, None))

            if par.name == "scw_list":
                self.logger.info("set_pars_from_dic>> scw_list is %s", par.value)

        if arg_dic.get('allow_unknown_args', None):
            self.allow_unknown_arguments = arg_dic.get('allow_unknown_args', 'False') == 'True'
        known_argument_names = non_parameter_args + self.get_arguments_name_list()
        self.unknown_arguments_name_list = []
        for k in list(updated_arg_dic.keys()):
            if k not in known_argument_names:
                if not self.allow_unknown_arguments:
                    updated_arg_dic.pop(k) 
                    self.logger.warning("argument '%s' is in the request but not used by instrument '%s', removing it", k, self.name)
                    self.unknown_arguments_name_list.append(k)
                else:
                    self.logger.warning("argument '%s' not defined for instrument '%s'", k, self.name)
        
        return updated_arg_dic

    def set_par(self,par_name,value):
        p=self.get_par_by_name(par_name)
        p.value=value

    def get_query_by_name(self, prod_name):
        p=None
        for _query in self._queries_list:
            if prod_name == _query.name:
                p = _query

        if p is None:
            sentry.capture_message(f'query for the product {prod_name} not found')
            raise Warning(f'query for the product {prod_name} not found')

        return p

    def test_communication(self, config, logger=None):
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
                           upload_dir,
                           products_url,
                           bind_host,
                           bind_port,
                           request_files_dir,
                           decoded_token,
                           sentry_dsn=None):
        error_message = 'Error while {step} {temp_dir_content_msg}{additional}'
        # TODO probably exception handling can be further improved and/or optmized
        try:
            # set catalog
            step = 'uploading catalog file from the frontend'
            cat_file_path = self.upload_catalog_from_fronted(par_dic=par_dic, request=request, temp_dir=temp_dir)
            if cat_file_path is not None:
                step = 'setting catalog file from the frontend'
            else:
                step = 'setting catalog object'
            self.set_catalog(par_dic)

            # set scw_list
            step = 'uploading scw_list file'
            input_file_path = self.upload_input_products_from_fronted(name='user_scw_list_file',
                                                                      request=request,
                                                                      temp_dir=temp_dir)
            step = 'setting input scw_list file'
            self.set_input_products_from_fronted(input_file_path=input_file_path, par_dic=par_dic, verbose=verbose)

            # any other file
            step = 'uploading other files'
            uploaded_files_obj = upload_files_request(request=request,
                                                       upload_dir=upload_dir)
            step = 'updating par_dic with the uploaded files'
            self.update_par_dic_with_uploaded_files(par_dic=par_dic,
                                                    uploaded_files_obj=uploaded_files_obj,
                                                    products_url=products_url,
                                                    bind_host=bind_host,
                                                    bind_port=bind_port)
            step = 'updating ownership files'
            self.update_ownership_files(uploaded_files_obj,
                                        request_files_dir=request_files_dir,
                                        decoded_token=decoded_token)

        except RequestNotUnderstood as e:
            error_message = error_message.format(step=step,
                                                 temp_dir_content_msg='',
                                                 additional=': '+getattr(e, 'message', ''))
            sentry.capture_message(f'{error_message}\n{e}')
            raise RequestNotUnderstood(error_message)
        except Exception as e:
            error_message = error_message.format(step=step,
                                                 temp_dir_content_msg='' if not os.path.exists(temp_dir) else
                                                 f', content of the temporary directory is {os.listdir(temp_dir)}',
                                                 additional=f'content of the request dict is {par_dic}')

            sentry.capture_message(f'{error_message}\n{e}')

            raise RequestNotUnderstood(error_message)

        if input_file_path is None and use_scws == 'user_file':
            error_message = 'scw_list file was expected to be passed, ' \
                            'but it has not been found, please check the inputs'

            raise RequestNotUnderstood(error_message)
        elif input_file_path is not None and use_scws != 'user_file':
            error_message = 'scw_list file was found despite ' \
                            'use_scws was indicating this was not provided, please check the inputs'

            raise RequestNotUnderstood(error_message)

    def get_status_details(self,
                           par_dic,
                           config=None,
                           logger=None):
        if logger is None:
            logger = self.get_logger()

        status_details_output_obj = {
            'status': 'successful'
        }

        # TODO put this in a dedicated function, perhaps within the oda_api
        # adaptation for oda_api, like it happens in oda_api set_api_code function
        updated_par_dic = par_dic.copy()
        updated_par_dic['product'] = updated_par_dic['product_type']
        updated_par_dic['product_type'] = updated_par_dic['query_type']
        updated_par_dic.pop('query_type')

        logger.info(f"getting products for a more in-depth analysis for the results within run_call_back with args {updated_par_dic}")
        disp = DispatcherAPI(url=config.dispatcher_callback_url_base, instrument='mock')
        try:
            disp.get_product(**updated_par_dic)
        except (DispatcherException,
                DispatcherNotAvailable,
                UnexpectedDispatcherStatusCode,
                RequestNotUnderstoodOdaApi) as de:
            logger.info('A problem has been detected when performing an assessment of the outcome of your request.\n'
                        'An exception regarding the dispatcher has been returned by the oda_api when retrieving '
                        'information from a completed job')
            status_details_output_obj['status'] = 'dispatcher_exception'
            status_details_output_obj['exception_message'] = str(de)
            sentry.capture_message(f'Dispatcher-related exception detected when retrieving additional '
                                   f'information from a completed job:\n{de}')
        except ConnectionError as ce:
            logger.info('A problem has been detected when performing an assessment of the outcome of your request.\n'
                        'A connection error has been detected when retrieving additional information '
                        f'from a completed job: {ce}')
            status_details_output_obj['status'] = 'connection_error'
            status_details_output_obj['exception_message'] = str(ce)
            sentry.capture_message(f'ConnectionError detected when retrieving additional '
                                   f'information from a completed job:\n{ce}')
        except Unauthorized as ue:
            detail_message = ""
            status_details_output_obj['status'] = 'authorization_error'
            if 'The token provided is expired' in ue.message:
                detail_message = ('It looks like the token has expired before the job completion, and therefore the request cannot be completed.\n'
                                  'The result might however be complete or mostly ready, please resubmit it using a token with longer validity.')
                status_details_output_obj['status'] = 'expired_token'
            # TODO probably not really needed ... ?
            elif 'The token provided is not valid' in ue.message:
                detail_message = ('It looks like the provided token is not valid, and therefore the request cannot be completed.\n'
                                  'The result might however be complete or mostly ready, please resubmit it using a valid token.')
                status_details_output_obj['status'] = 'invalid_token'
            logger.info('A problem has been detected when performing an assessment of the outcome of your request.\n'
                        f'{detail_message}\n'
                        f'For more information you can contact us at the contact@odahub.io email address or using the dedicated form.')
            status_details_output_obj['exception_message'] = str(ue)
            sentry.capture_message(f'Authorization-related exception detected when retrieving additional '
                                   f'information from a completed job:\n{ue}')
        except RemoteException as re:
            if 'unable to complete API call' in re.message:
                logger.info('A problem has been detected when performing an assessment of the outcome of your request.\n'
                            'A connection error has been detected and therefore this research could not be completed successfully.')

                status_details_output_obj['status'] = 'connection_error'
                status_details_output_obj['exception_message'] = re.message + '\n' + re.debug_message
            elif 'remote/connection error, server response is not valid' in re.message:
                logger.info('A problem has been detected when performing an assessment of the outcome of your request.\n'
                            'This research has detected that an empty result has been produced.\n'
                            'Please look carefully on your request.')

                status_details_output_obj['status'] = 'empty_result'
                status_details_output_obj['exception_message'] = re.message + '\n' + re.debug_message
            else:
                logger.info('A problem has been detected when performing an assessment of the outcome of your request.\n'
                            'This most likely contains an empty product.')

                status_details_output_obj['status'] = 'empty_product'
                status_details_output_obj['exception_message'] = re.message + '\n' + re.debug_message

            sentry.capture_message(f'RemoteException detected when retrieving additional '
                                   f'information from a completed job:\n{re}')

        return status_details_output_obj

    def run_query(self, product_type,
                  par_dic,
                  back_end_query,
                  job,
                  run_asynch,
                  config=None,
                  out_dir=None,
                  query_type='Real',
                  verbose=False,
                  logger=None,
                  sentry_dsn=None,
                  dry_run=False,
                  api=False,
                  decoded_token=None,
                  return_progress=False,
                  **kwargs):

        if logger is None:
            logger = self.get_logger()

        #  this was removed by 2f5b5dfb7e but turns out it is used by some plugins, see test_server_plugin_integral_all_sky
        self._current_par_dic=par_dic

        # # set pars values from the input parameters
        # query_out = self.set_pars_from_form(par_dic, verbose=verbose, sentry_dsn=sentry_dsn)
        query_out = QueryOutput()
        query_out.set_done()
        if verbose:
            self.show_parameters_list()

        logger.info('--> par dict', par_dic)

        if dry_run:
            job.set_done()
            if query_out.status_dictionary['status'] == 0:
                query_out.set_done(message='dry-run',job_status=job.status)
                query_out.set_instrument_parameters(self.get_parameters_list_jsonifiable(prod_name=product_type))
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

                    query_out = query_obj.run_query(self, out_dir, job, run_asynch,
                                                    query_type=query_type,
                                                    config=config,
                                                    logger=logger,
                                                    sentry_dsn=sentry_dsn,
                                                    return_progress=return_progress,
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
                except ProductProcessingError as e:
                    logger.warning("error in the post processing of the products to fail, passing through: %s", e)
                    query_out.set_status(1, message="Error during the products post processing",
                                         error_message=str(e))
                except InternalError as e:
                    if hasattr(e, 'message') and e.message is not None:
                        message = e.message
                        tail_message = ('The support team has been notified, '
                                        'and we are investigating to resolve the issue as soon as possible\n\n'
                                        'If you are willing to help us, please use the "Write a feedback" button below. '
                                        'We will make sure to respond to any feedback provided')
                    else:
                        message = ('Your request produced an unusual result. It might not be what you expected. '
                                     'It is possible that this particular parameter selection should indeed lead to this outcome '
                                     '(e.g. there is no usable data). Please look carefully on your request.\n\n'
                                     'It is also possible that the platform experienced a temporary issue. '
                                     'We aim at distinguishing all of such issues and report them clearly, '
                                     'but for now, we unfortunately can not be certain all cases like this are detected. '
                                     'We try to discover on our own and directly address any temporary issue. '
                                     'But some issues might slip past us. If you are willing to help us, '
                                     'please use "Write a feedback" button below. We would greatly appreciate it!\n\n'
                                     'This additional information might help:\n\n'
                               )
                        tail_message = ''
                    e_message = f'Instrument: {self.name}, product: {product_type}\n\n{tail_message}'

                    debug_message = ''
                    if e.payload is not None and e.payload.get('exception', None) is not None:
                        debug_message = repr(e.payload['exception'])

                    query_out.set_failed(product_type,
                                         message=message,
                                         e_message=e_message,
                                         logger=logger,
                                         sentry_dsn=sentry_dsn,
                                         debug_message=debug_message,
                                         excep=e)

                except Exception as e: # we shall not do that
                    logger.error("run_query failed: %s", e)
                    # logger.error("run_query failed: %s", traceback.format_exc())
                    query_out.set_failed(product_type, logger=logger, sentry_dsn=sentry_dsn, excep=e)

        # adding query parameters to final products
        # TODO: this can be misleading since it's the parameters actually used
        query_out.set_analysis_parameters(par_dic)
        # TODO perhaps this will change
        query_out.set_api_code(par_dic, url=back_end_query.config.products_url + "/dispatch-data")
        query_out.dump_analysis_parameters(out_dir, par_dic)

        return query_out

    def get_product_query_name(self, product_type):
        if product_type not in self.query_dictionary:
            sentry.capture_message(f'product type {product_type} not in query_dictionary {self.query_dictionary}')
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

    def get_html_draw(self, prod_name, image, image_header, catalog=None, **kwargs):
        return self.get_query_by_name(prod_name).get_html_draw(image, image_header, catalog=catalog, **kwargs)

    def get_par_by_name(self, par_name, add_src_query=True, add_instr_query=True, prod_name=None):
        p=None
        
        for _query in self._queries_list:
            if isinstance(_query, SourceQuery) and not add_src_query:
                continue
            
            if isinstance(_query, InstrumentQuery) and not add_instr_query:
                continue
            
            if isinstance(_query, ProductQuery) and prod_name is not None and _query.name!=self.query_dictionary[prod_name]:
                continue

            if par_name in _query.par_names:
                if p is not None:
                    self.logger.warning('Same parameter name %s in several queries. '
                                        'Will return parameter from the last query')
                p  =  _query.get_par_by_name(par_name)

        if p is None:
            raise Warning('parameter', par_name, 'not found')

        return p

    def show_parameters_list(self):

        print ("-------------")
        for _query in self._queries_list:
            print ('q:',_query.name)
            _query.show_parameters_list()
        print("-------------")

    def get_parameters_list_jsonifiable(self, add_src_query=True, add_instr_query=True, prod_name=None):

        l=[{'instrumet':self.name}]
        l.append({'prod_dict':self.query_dictionary})

        for _query in self._queries_list:
            if isinstance(_query,SourceQuery) and not add_src_query:
                continue

            if isinstance(_query,InstrumentQuery) and not add_instr_query:
                continue

            if isinstance(_query, ProductQuery) and prod_name is not None and _query.name!=self.query_dictionary[prod_name]:
                continue

            l.append(_query.get_parameters_list_jsonifiable(prod_dict=self.query_dictionary))

        return l
    
    def get_parameters_list(self, prod_name=None):
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

            if _add_query:
                l.extend(_query._parameters_list)

        return l
    
    def get_arguments_name_list(self, prod_name=None):
        l = []
        for par in self.get_parameters_list(prod_name = prod_name):
            l.extend(par.argument_names_list)
        l = list(dict.fromkeys(l)) # remove duplicates preserving order
        return l
        
    def get_parameters_name_list(self, prod_name=None):
        l = []
        for par in self.get_parameters_list(prod_name = prod_name):
            l.append(par.name)
        if len(l) > len(set(l)):
            self.logger.warning('duplicates in parameters_name_list: %s', l)
        return l

    # TODO this seems not being used anywhere on the dispatcher, can it be removed?
    def set_pars_from_form(self,par_dic,logger=None,verbose=False,sentry_dsn=None):
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
                        sentry_dsn=sentry_dsn,
                        excep=e)

        except Exception as e:
            #FAILED
            m = f'problem setting form parameters from dict: {par_dic}'
            logger.error(m)

            q.set_failed(m,
                         logger=logger,
                         sentry_dsn=sentry_dsn,
                         excep=e)
            #status=1
            #error_message= 'error in form parameter'
            #debug_message = e
            #logger.exception(e)
        #print('---------------------------------------------')
        return q

    def upload_input_products_from_fronted(self, name, request, temp_dir):
        input_file_path = None
        if request.method == 'POST':
            # save to a temporary folder, and delete it afterwards
            input_file_path = upload_file(name, temp_dir)
        return input_file_path

    def set_input_products_from_fronted(self, input_file_path, par_dic, verbose=False):
        input_prod_list_name = self.instrumet_query.input_prod_list_name
        if input_file_path is not None:
            has_input = self.set_input_products(par_dic, input_file_path, input_prod_list_name)
            if has_input:
                pass
            else:
                raise RuntimeError

    def update_par_dic_with_uploaded_files(self, par_dic, uploaded_files_obj, products_url, bind_host, bind_port):
        if validators.url(products_url, simple_host=True):
            # TODO remove the dispatch-data part, better to have it extracted from the configuration file
            basepath = os.path.join(products_url, 'dispatch-data/download_file')
        else:
            basepath = os.path.join(f"http://{bind_host}:{bind_port}", 'download_file')
        for f in uploaded_files_obj:
            dict_args = dict(file_list=uploaded_files_obj[f], _is_mmoda_url=True, return_archive=False)
            dpars = urlencode(dict_args)
            download_file_url = f"{basepath}?{dpars}"
            par_dic[f] = download_file_url

    def update_ownership_files(self, uploaded_files_obj, request_files_dir, decoded_token=None):
        if decoded_token is not None:
            user_email = tokenHelper.get_token_user_email_address(decoded_token)
            user_roles = tokenHelper.get_token_roles(decoded_token)
        else:
            user_email = 'public'
            user_roles = []

        update_file = False
        for file_name in uploaded_files_obj:
            file_hash = uploaded_files_obj[file_name]
            ownership_file_path = os.path.join(request_files_dir, f'{file_hash}_ownerships.json')
            if not os.path.exists(ownership_file_path):
                ownerships = dict(
                    user_emails=[user_email],
                    user_roles=user_roles
                )
                update_file = True
            else:
                with open(ownership_file_path) as ownership_file:
                    ownerships = json.load(ownership_file)
            if user_email not in ownerships['user_emails']:
                ownerships['user_emails'].append(user_email)
                update_file = True
            if not all(role in ownerships['user_roles'] for role in user_roles):
                set_user_roles = set(ownerships['user_roles'])
                set_user_roles |= set(user_roles)
                ownerships['user_roles'] = list(set_user_roles)
                update_file = True
            if update_file:
                with open(ownership_file_path, 'w') as ownership_file:
                    json.dump(ownerships, ownership_file)

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
                        # check if they are space-separated, and in case raise exception since it's unsupported
                        ll_space_separated = ll.split()
                        if len(ll_space_separated) > 1:
                            raise RequestNotUnderstood('a space separated science windows list is an unsupported format, '
                                                       'please provide it as a comme separated list')
                        lines.extend(ll.split(","))
                    lines = [item.strip() for item in lines]
                    cleaned_lines=[]
                    for line in lines:
                        line = ''.join([x if x in string.printable else '' for x in line])
                        cleaned_lines.append(line)

                par_dic[input_prod_list_name] = cleaned_lines
                has_prods= len(lines) >= 1
            except RequestNotUnderstood as e:
                raise e
            except Exception:
                has_prods=False

        return has_prods


    def upload_catalog_from_fronted(self, par_dic, request, temp_dir):
        cat_file_path = None
        if request.method == 'POST':
            # save to a temporary folder, and delete it afterwards
            cat_file_path = upload_file('user_catalog_file', temp_dir)
            if cat_file_path is not None:
                par_dic['user_catalog_file'] = cat_file_path
        return cat_file_path

    def set_catalog(self, par_dic):
        # setting user_catalog in the par_dic, either loading it from the file or aas an object
        if 'user_catalog_file' in par_dic.keys() and par_dic['user_catalog_file'] is not None:
            user_catalog_file = par_dic['user_catalog_file']
            try:
                catalog_object = load_user_catalog(user_catalog_file)
            except RuntimeError:
                raise RequestNotUnderstood('format not valid, a catalog should be provided as a FITS (typical standard OSA catalog) or '
                                           '<a href=https://docs.astropy.org/en/stable/api/astropy.io.ascii.Ecsv.html>ECSV</a> table.')
            # normalize catalog
            normalized_user_catalog = normalize_catalog(catalog_object)

            self.set_par('user_catalog', normalized_user_catalog)
            self.set_par('selected_catalog', json.dumps(normalized_user_catalog.get_dictionary()))
            # not needed in the frontend
            par_dic.pop('user_catalog_file', None)
        else:
            # TODO comes from the frontend when user selects "use as catalog"
            if 'catalog_selected_objects' in par_dic.keys():
                try:
                    catalog_selected_objects = np.array(par_dic['catalog_selected_objects'].split(','), dtype=int)
                except:
                    # TODO of course to provide a better message
                    raise RequestNotUnderstood("the selected catalog is wrongly formatted, please check your inputs")
            else:
                catalog_selected_objects = None

            if 'selected_catalog' in par_dic.keys():
                catalog_dic = json.loads(par_dic['selected_catalog'])
                try:
                    user_catalog = build_catalog(catalog_dic, catalog_selected_objects)
                except ValueError as e:
                    e_message = str(e)
                    raise RequestNotUnderstood(e_message)
                # normalize catalog
                normalized_user_catalog = normalize_catalog(user_catalog)
                self.set_par('user_catalog', normalized_user_catalog)


# TODO consider if better to move those three functions inside catalog module

def load_user_catalog(user_catalog_file):
    return BasicCatalog.from_file(user_catalog_file)


# TODO here I would apply the normalization
def normalize_catalog(catalog_object):
    if isinstance(catalog_object, BasicCatalog):
        catalog_object = catalog_object.get_dictionary()
    normalized_catalog_object = dict()

    return build_catalog(normalized_catalog_object)


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

class InstrumentFactoryIterator:
    def __init__(self):
        self._partlist = []
        
    def extend(self, lst):
        self._partlist.append(lst)
    
    def __iter__(self):
        return (y for x in self._partlist for y in x)