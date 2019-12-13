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


import json
import  logging
import  re
import yaml

import os
import  numpy as np
from astropy.table import Table

from cdci_data_analysis.analysis.queries import _check_is_base_query
from .catalog import BasicCatalog
from .products import  QueryOutput
from .queries import ProductQuery,SourceQuery,InstrumentQuery
from .io_helper import FilePath

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f



class Instrument(object):
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

        #name
        self.name=instr_name

        #src query
        self.src_query=src_query


        self.asynch=asynch

        #Instrument specific
        self.instrumet_query=instrumet_query

        #self.data_serve_conf_file=data_serve_conf_file
        self.set_data_server_conf_dict(data_serve_conf_file)

        self.product_queries_list=product_queries_list

        self._queries_list=[self.src_query,self.instrumet_query]


        self.data_server_query_class=data_server_query_class

        if product_queries_list is not None and product_queries_list !=[]:
            self._queries_list.extend(product_queries_list)

        _check_is_base_query(self._queries_list)

        self.input_product_query=input_product_query

        self.query_dictionary = query_dictionary


    def set_data_server_conf_dict(self,data_serve_conf_file):
        conf_dict=None
        #print ('--> setting set_data_server_conf_dict for', self.name,'from data_serve_conf_file',data_serve_conf_file)
        if data_serve_conf_file is not None:
           with open(data_serve_conf_file, 'r') as ymlfile:
                cfg_dict = yaml.load(ymlfile)
                for k in cfg_dict['instruments'].keys():
                    #print ('name',k)
                    if self.name ==k:
                        #print('name', k,cfg_dict['instruments'][k])
                        conf_dict=cfg_dict['instruments'][k]

        self.data_server_conf_dict=conf_dict

    def get_logger(self):
        logger = logging.getLogger(__name__)
        return logger


    def _check_names(self):
        pass



    def set_pars_from_dic(self,par_dic,verbose=False):

        for _query in self._queries_list:

            for par in _query._parameters_list:
                par.set_from_form(par_dic,verbose=verbose)


    def set_par(self,par_name,value):
        p=self.get_par_by_name(par_name)
        p.value=value




    def get_query_by_name(self,prod_name):
        p=None
        for _query in self._queries_list:
            if prod_name == _query.name:
                p  =  _query

        if p is None:
            raise Warning('query', prod_name, 'not found')

        return p

    def test_communication(self,config,logger=None):
        if self.data_server_query_class is not None:
            return self.data_server_query_class(config=config,instrument=self).test_communication(logger=logger)

    def test_busy(self, config,logger=None):
        if self.data_server_query_class is not None:
            return self.data_server_query_class(config=config).test_busy(logger=logger)

    def test_has_input_products(self, config,instrument,logger=None):
        if self.data_server_query_class is not None:
            return self.data_server_query_class(config=config,instrument=self).test_has_input_products(instrument,logger=logger)



    def run_query(self,product_type,
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
                  **kwargs):

        #prod_dictionary={}

        self._current_par_dic=par_dic

        if logger is None:
            logger = self.get_logger()

        #set pars
        query_out=self.set_pars_from_form(par_dic,verbose=verbose,sentry_client=sentry_client)


        if verbose ==True:
            self.show_parameters_list()




        #set catalog
        if query_out.status_dictionary['status']==0:
            query_out=self.set_catalog_from_fronted(par_dic, request,back_end_query,logger=logger,verbose=verbose,sentry_client=sentry_client)


        #set input products
        if query_out.status_dictionary['status'] == 0:
            try:
                query_out=self.set_input_products_from_fronted(par_dic, request,back_end_query,logger=logger,verbose=verbose,sentry_client=sentry_client)
            except Exception as e:
                # FAILED
                query_out.set_failed(product_type,message='wrong parameter', logger=logger, sentry_client=sentry_client, excep=e)

        if dry_run == True:
            job.set_done()
            if query_out.status_dictionary['status'] == 0:
                query_out.set_done(message='dry-run',job_status=job.status)
                query_out.set_instrument_parameters(self.get_parameters_list_as_json(prod_name=product_type))
        else:
            if query_out.status_dictionary['status'] == 0:
                #print('--->CICCIO',self.query_dictionary)

                query_out = QueryOutput()
                message=''
                debug_message=''

                try:
                    query_name = self.query_dictionary[product_type]
                    #print ('=======> query_name',query_name)
                    query_out = self.get_query_by_name(query_name).run_query(self, out_dir, job, run_asynch,
                                                                             query_type=query_type, config=config,
                                                                             logger=logger,
                                                                             sentry_client=sentry_client,
                                                                             api=api)
                    if query_out.status_dictionary['status'] == 0:
                        #DONE
                        if 'comment' in query_out.status_dictionary.keys():
                            backend_comment = query_out.status_dictionary['comment']
                        else:
                            backend_comment = ''
                        if 'warning' in query_out.status_dictionary.keys():
                            backend_warning = query_out.status_dictionary['warning']
                        else:
                            backend_warning = ''

                        query_out.set_done(message=message,
                                           debug_message=str(debug_message),
                                           comment=backend_comment,
                                           warning=backend_warning)
                    else:
                        pass

                except Exception as e:
                    #FAILED
                    query_out.set_failed(product_type,logger=logger,sentry_client=sentry_client,excep=e)



        #adding query parameters to final products
        query_out.set_analysis_parameters(par_dic)
        query_out.set_api_code(par_dic)
        query_out.dump_analysis_parameters(out_dir,par_dic)

        return query_out



    def get_html_draw(self, prod_name, image,image_header,catalog=None,**kwargs):

        return self.get_query_by_name(prod_name).get_html_draw( image,image_header,catalog=catalog,**kwargs)

    def get_par_by_name(self,par_name):
        p=None

        for _query in self._queries_list:
            if par_name in _query.par_names:
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


    def get_parameters_name_list(self, prod_name= None):
        l=[]
        _add_query = False
        for _query in self._queries_list:
            _add_query = True
            print('q:', _query.name)
            if isinstance(_query,InstrumentQuery) :
                _add_query=False
            #print('isntr', _query.name)

            if isinstance(_query, ProductQuery) and prod_name is not None and _query.name==self.query_dictionary[prod_name]:
                _add_query = True
                #print('prd', _query.name,prod_name)
            elif isinstance(_query, ProductQuery) and prod_name is not None and _query.name!=self.query_dictionary[prod_name]:
                #print('prd', _query.name, prod_name)
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
        except Exception as e:
            #FAILED
            q.set_failed('setting form parameters',logger=logger,sentry_client=sentry_client,excep=e)

            #status=1
            #error_message= 'error in form parameter'
            #debug_message = e
            #logger.exception(e)


        #print('---------------------------------------------')
        return q


    def set_input_products_from_fronted(self,par_dic,request,back_end_query,verbose=False,logger=None,sentry_client=None):
        #print('---------------------------------------------')
        #print('setting user input prods')
        input_prod_list_name = self.instrumet_query.input_prod_list_name
        q = QueryOutput()
        #status = 0
        error_message = ''
        debug_message = ''
        input_file_path=None

        if logger is None:
            logger = logging.getLogger(__name__)

        if request.method == 'POST':
            try:
                input_file_path = back_end_query.upload_file('user_scw_list_file', back_end_query.scratch_dir)
                #DONE
                q.set_done( debug_message=str(debug_message))
            except Exception as e:
                #DONE
                q.set_failed('failed to upload scw_list file',
                             extra_message='failed to upload %s' % self.input_prod_name,
                             sentry_client=sentry_client,
                             excep=e)


            try:
                has_input=self.set_input_products(par_dic,input_file_path,input_prod_list_name)
                #DONE
                q.set_done( debug_message=str(debug_message))
            except Exception as e :
                #FAILED
                q.set_failed('scw_list file is not valid',
                             extra_message='scw_list file is not valid, please check the format',
                             logger=logger,
                             sentry_client=sentry_client,
                             excep=e)



            #print ('has input',has_input)
            try:

                if has_input==True:
                    pass
                else:
                    raise RuntimeError
                #DONE
                q.set_done( debug_message=str(debug_message))

            except Exception as e:
                #FAILED
                q.set_failed('setting input scw_list',
                             extra_message='scw_list file is not valid, please check the format',
                             sentry_client=sentry_client,
                             excep=e)


        self.set_pars_from_dic(par_dic,verbose=verbose)

        #print('---------------------------------------------')
        return q






    def set_input_products(self, par_dic, input_file_path,input_prod_list_name):
        has_prods=False
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
                par_dic[input_prod_list_name] = lines
                has_prods= len(lines) >= 1
            except:
                has_prods=False

        return has_prods

        # template = re.compile(r'^(\d{12}).(\d{3})$')
        # if input_file_path is None:
        #     return True
        # else:
        #     with open(input_file_path) as f:
        #         _lines = f.readlines()
        #
        #     # should now accept any combination of commas and/or newlines
        #     # raise error if any of the scwlist is not matching the template
        #     lines = []
        #     for ll in _lines:
        #         lines.extend(ll.split(","))
        #
        #
        #     acceptList = [item.strip() for item in lines if template.match(item)]
        #
        #
        #
        #
        #     if len(acceptList)!=len(lines):
        #         raise RuntimeError
        #
        #     par_dic[input_prod_list_name]=acceptList
        #     #print ("--> accepted scws",acceptList,len(acceptList))
        #     return len(acceptList)>=1


    def set_catalog_from_fronted(self,par_dic,request,back_end_query,logger=None,verbose=False,sentry_client=None):
        #print('---------------------------------------------')
        #print('setting user catalog')

        q = QueryOutput()
        #status = 0
        error_message = ''
        debug_message = ''

        if logger is None:
            logger = logging.getLogger(__name__)

        cat_file_path=None
        if request.method == 'POST':
            #print('POST')
            try:
                cat_file_path = back_end_query.upload_file('user_catalog_file', back_end_query.scratch_dir)
                par_dic['user_catalog_file'] = cat_file_path
                #print('set_catalog_from_fronted,request.method', request.method, par_dic['user_catalog_file'],cat_file_path)
                #DONE
                q.set_done( debug_message=str(debug_message))
            except Exception as e:
                #FAILED
                q.set_failed('upload catalog file',
                             extra_message='failed to upload catalog file',
                             logger=logger,
                             sentry_client=sentry_client,
                             excep=e)



        try:
            self.set_catalog(par_dic, scratch_dir=back_end_query.scratch_dir)
            #DONE
            q.set_done(debug_message=str(debug_message))
        except Exception as e:
            # FAILED
            q.set_failed('set catalog file',
                         extra_message='failed to set catalog',
                         logger=logger,
                         sentry_client=sentry_client,
                         excep=e)



        self.set_pars_from_dic(par_dic,verbose=verbose)

        #print('setting user catalog done')
        #print('---------------------------------------------')
        return q

    def set_catalog(self, par_dic, scratch_dir='./'):

        user_catalog_file=None
        if 'user_catalog_file' in par_dic.keys() :
            user_catalog_file = par_dic['user_catalog_file']
            #print("--> user_catalog_file ",user_catalog_file)

        if 'user_catalog_dictionary'in par_dic.keys() and par_dic['user_catalog_dictionary'] is not None:
            if type(par_dic['user_catalog_dictionary'])==dict:
                self.set_par('user_catalog',build_catalog(par_dic['user_catalog_dictionary']))
            else:
                catalog_dic = json.loads(par_dic['selected_catalog'])
                self.set_par('user_catalog', build_catalog(catalog_dic))

        if user_catalog_file is not None:
            # print('loading catalog  using file', user_catalog_file)
            self.set_par('user_catalog', load_user_catalog(user_catalog_file))
            # print('user catalog done, using file',user_catalog_file)

        else:
            if 'catalog_selected_objects' in par_dic.keys():

                catalog_selected_objects = np.array(par_dic['catalog_selected_objects'].split(','), dtype=np.int)
            else:
                catalog_selected_objects = None

            if 'selected_catalog' in par_dic.keys():
                catalog_dic=json.loads(par_dic['selected_catalog'])

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