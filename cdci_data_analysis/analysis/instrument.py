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

from werkzeug.utils import secure_filename

from pathlib import Path
import json
import  logging
import  re
#logger = logging.getLogger(__name__)

import  numpy as np
from astropy.table import Table

from cdci_data_analysis.analysis.queries import _check_is_base_query
from .catalog import BasicCatalog
from .products import  QueryOutput
from .io_helper import view_traceback

import  os

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
                 catalog=None,
                 product_queries_list=None,
                 data_server_query_class=None,
                 query_dictionary={},
                 max_pointings=None):

        #name
        self.name=instr_name

        #src query
        self.src_query=src_query


        #Instrument specific
        self.instrumet_query=instrumet_query



        self.product_queries_list=product_queries_list

        self._queries_list=[self.src_query,self.instrumet_query]


        self.data_server_query_class=data_server_query_class

        if product_queries_list is not None and product_queries_list !=[]:
            self._queries_list.extend(product_queries_list)

        _check_is_base_query(self._queries_list)

        self.input_product_query=input_product_query

        self.query_dictionary = query_dictionary

        self.max_pointings=max_pointings



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

    def test_communication(self,config):
        if self.data_server_query_class is not None:
            return self.data_server_query_class(config=config).test_connection()

    def test_busy(self, config):
        if self.data_server_query_class is not None:
            return self.data_server_query_class(config=config).test_busy()

    def test_has_input_products(self, config,instrument):
        if self.data_server_query_class is not None:
            return self.data_server_query_class(config=config).test_has_input_products(instrument)

    def run_query(self,product_type,par_dic,request,back_end_query,job,prompt_delegate,config=None,out_dir=None,query_type='Real',verbose=False,logger=None,**kwargs):

        #prod_dictionary={}


        if logger is None:
            logger = logging.getLogger(__name__)

        #set pars

        query_out=self.set_pars_from_form(par_dic,verbose=verbose)

        if verbose ==True:
            self.show_parameters_list()



        #set catalog
        if query_out.status_dictionary['status']==0:
            query_out=self.set_catalog_from_fronted(par_dic, request,back_end_query,logger=logger,verbose=verbose)


        #set input products
        if query_out.status_dictionary['status'] == 0:
            query_out=self.set_input_products_from_fronted(par_dic, request,back_end_query,logger=logger,verbose=verbose)




        if query_out.status_dictionary['status'] == 0:
            #print('--->CICCIO',self.query_dictionary)

            query_out = QueryOutput()
            status=0
            message=''
            debug_message=''

            try:
                query_name = self.query_dictionary[product_type]
                query_out = self.get_query_by_name(query_name).run_query(self, out_dir, job, prompt_delegate,
                                                                         query_type=query_type, config=config,
                                                                         logger=logger)
            except Exception as e:

                print('!!! >>>Exception<<<', e)
                print("product error", e)
                view_traceback()
                logger.exception(e)
                status = 1
                message = 'product error: %s'%(product_type)
                debug_message = e

                msg_str = '==>product error:',e
                logger.info(msg_str)

                query_out.set_status(status, message, debug_message=str(debug_message))






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


    def get_parameters_list_as_json(self):
        l=[{'instrumet':self.name}]
        for _query in self._queries_list:
            l.append(_query.get_parameters_list_as_json())

        return l




    def set_pars_from_form(self,par_dic,logger=None,verbose=False):
        print('---------------------------------------------')
        print('setting form paramters')
        q=QueryOutput()
        status=0
        error_message=''
        debug_message=''
        if logger is None:
            logger = logging.getLogger(__name__)

        try:
            self.set_pars_from_dic(par_dic,verbose=verbose)
        except Exception as e:
            status=1
            error_message= 'error in form parameter'
            debug_message = e
            logger.exception(e)

        q.set_status(status,error_message,str(debug_message))
        print('---------------------------------------------')
        return q


    def set_input_products_from_fronted(self,par_dic,request,back_end_query,verbose=False,logger=None):
        print('---------------------------------------------')
        print('setting user input prods')
        input_prod_list_name = self.instrumet_query.input_prod_list_name
        q = QueryOutput()
        status = 0
        error_message = ''
        debug_message = ''
        input_file_path=None

        if logger is None:
            logger = logging.getLogger(__name__)

        if request.method == 'POST':
            try:
                input_file_path = back_end_query.upload_file('user_scw_list_file', back_end_query.scratch_dir)


            except Exception as e:
                error_message = 'failed to upload %s'%self.input_prod_name
                status = 1
                debug_message = e
                logger.exception(e)

            try:
                has_input=self.set_input_products(par_dic,input_file_path,input_prod_list_name)
            except Exception as e :
                error_message = 'scw_list file is not valid'
                status = 1
                debug_message = e
                logger.exception(e)

            print ('has input',has_input)
            try:
                if has_input==True:
                    pass
                else:
                    raise RuntimeError
            except:
                error_message = 'No scw_list from file accepted'
                status = 1
                debug_message = 'no valid scw in the scwlist file'
                logger.exception(debug_message)

        self.set_pars_from_dic(par_dic,verbose=verbose)
        q.set_status(status, error_message, str(debug_message))
        print('---------------------------------------------')
        return q






    def set_input_products(self, par_dic, input_file_path,input_prod_list_name):
        template = re.compile(r'^(\d{12}).(\d{3})$')
        if input_file_path is None:
            return True
        else:
            with open(input_file_path) as f:
                lines = f.readlines()

            acceptList = [item.strip() for item in lines if template.match(item)]
            par_dic[input_prod_list_name]=acceptList
            print ("--> accepted scws",acceptList,len(acceptList))
            return len(acceptList)>=1


    def set_catalog_from_fronted(self,par_dic,request,back_end_query,logger=None,verbose=False):
        print('---------------------------------------------')
        print('setting user catalog')

        q = QueryOutput()
        status = 0
        error_message = ''
        debug_message = ''

        if logger is None:
            logger = logging.getLogger(__name__)

        cat_file_path=None
        if request.method == 'POST':
            print('POST')
            try:
                cat_file_path = back_end_query.upload_file('user_catalog_file', back_end_query.scratch_dir)
                par_dic['user_catalog_file'] = cat_file_path
                print('set_catalog_from_fronted,request.method', request.method, par_dic['user_catalog_file'],cat_file_path)
            except Exception as e:
                error_message = 'failed to upload catalog file'
                status = 1
                debug_message=e
                logger.exception(e)

        try:
            self.set_catalog(par_dic, scratch_dir=back_end_query.scratch_dir)

        except Exception as e:
            error_message = 'failed to set catalog '
            status = 1
            debug_message = e
            print(e)
            logger.exception(e)

        self.set_pars_from_dic(par_dic,verbose=verbose)
        q.set_status(status, error_message, str(debug_message))
        print('setting user catalog done')
        print('---------------------------------------------')
        return q

    def set_catalog(self, par_dic, scratch_dir='./'):

        user_catalog_file=None
        if 'user_catalog_file' in par_dic.keys() :
            user_catalog_file = par_dic['user_catalog_file']
            print("--> user_catalog_file ",user_catalog_file)

        if 'user_catalog_dictionary'in par_dic.keys() and par_dic['user_catalog_dictionary'] is not None:
            self.set_par('user_catalog',build_catalog(par_dic['user_catalog_dictionary']))
            print("user_catalog_dictionary ", par_dic['user_catalog_dictionary'])

        if user_catalog_file is not None:
            print('loading catalog  using file', user_catalog_file)
            self.set_par('user_catalog', load_user_catalog(user_catalog_file))
            print('user catalog done, using file',user_catalog_file)

        else:
            if 'catalog_selected_objects' in par_dic.keys():

                catalog_selected_objects = np.array(par_dic['catalog_selected_objects'].split(','), dtype=np.int)
            else:
                catalog_selected_objects = None

            if 'selected_catalog' in par_dic.keys():
                catalog_dic=json.loads(par_dic['selected_catalog'])
                print('==> selecetd catalog', catalog_dic)
                print('==> catalog_selected_objects', catalog_selected_objects)

                if catalog_selected_objects is not None:


                    user_catalog=build_catalog(catalog_dic,catalog_selected_objects)
                    self.set_par('user_catalog', user_catalog)
                    print('==> selecetd catalog')
                    print (user_catalog.table)




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

    if catalog_selected_objects is not None:
        meta_ids = user_catalog._table['meta_ID']
        IDs=[]
        for ID,cat_ID in enumerate(meta_ids):
            #print ("ID,cat_id",ID,cat_ID,catalog_selected_objects)
            if cat_ID in catalog_selected_objects:
                IDs.append(ID)
                #print('selected')

            user_catalog.select_IDs(IDs)

    return user_catalog