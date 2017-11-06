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
                 catalog=None,
                 product_queries_list=None,
                 data_server_query_class=None,
                 query_dictionary={}):

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

        self.query_dictionary = query_dictionary



    def _check_names(self):
        pass



    def set_pars_from_dic(self,par_dic):
        print(par_dic.keys())
        for _query in self._queries_list:
            for par in _query._parameters_list:
                par.set_from_form(par_dic)

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




    def run_query(self,product_type,par_dic,request,back_end_query,config=None,out_dir=None,query_type='Real',logger=None,**kwargs):

        prod_dictionary={}

        if logger is None:
            logger = logging.getLogger(__name__)

        prod_dictionary=self.set_pars_from_from(par_dic)
        self.instrument.show_parameters_list()

        if prod_dictionary['status']==0:
            prod_dictionary=self.set_catalog_from_fronted(par_dic, request,back_end_query,logger)

        if prod_dictionary['status'] == 0:

            prod_dictionary=self.set_input_products_from_fronted(par_dic, request,back_end_query,logger)

        if prod_dictionary['status'] == 0:
            query_name=self.query_dictionary[product_type]
            prod_dictionary=self.get_query_by_name(query_name).run_query(self,out_dir,query_type=query_type,config=config,logger=logger)

        return prod_dictionary



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




    def set_pars_from_from(self,par_dic,logger=None):
        prod_dictionary = {}
        if logger is None:
            logger = logging.getLogger(__name__)
        try:
            self.set_pars_from_dic(par_dic)
        except Exception as e:
            prod_dictionary['error_message'] = 'error form in parameter'
            prod_dictionary['status'] = 1
            logger.exception(e.message)

        return prod_dictionary


    def set_input_products_from_fronted(self,par_dic,request,back_end_query,logger=None):
        input_prod_list_name = self.instrumet_query.input_prod_list_name
        if logger is None:
            logger = logging.getLogger(__name__)

        if request.method == 'POST':
            prod_dictionary = {}
            try:
                input_file_path = back_end_query.upload_file(input_prod_list_name, back_end_query.scratch_dir)
                par_dic[input_prod_list_name] = input_file_path
            except Exception as e:
                prod_dictionary['error_message'] = 'failed to upload %s'%self.input_prod_name
                prod_dictionary['status'] = 1
                logger.exception(e.message)

        try:
            self.set_input_products(par_dic, scratch_dir=back_end_query.scratch_dir)
        except Exception as e:
            prod_dictionary['error_message'] = '%s file is not valid'%input_prod_list_name
            prod_dictionary['status'] = 1
            print(e.message)
            logger.exception(e.message)

        return  prod_dictionary






    def set_input_products(self, par_dic, input_file_path,input_prod_list_name):
        template = re.compile(r'^(\d{12}).(\d{3})$')
        if input_file_path is None:
            pass
        else:
            with open(input_file_path) as f:
                lines = f.readlines()

            acceptList = [item for item in lines if template.match(item)]
            par_dic[input_prod_list_name]=acceptList


    def set_catalog_from_fronted(self,par_dic,request,back_end_query,logger=None):
        if logger is None:
            logger = logging.getLogger(__name__)

        if request.method == 'POST':
            prod_dictionary = {}
            try:
                cat_file_path = back_end_query.upload_file('user_catalog', back_end_query.scratch_dir)
                par_dic['user_catalog'] = cat_file_path
            except Exception as e:
                prod_dictionary['error_message'] = 'failed to upload catalog'
                prod_dictionary['status'] = 1
                logger.exception(e.message)

        try:
            self.set_catalog(par_dic, scratch_dir=back_end_query.scratch_dir)
        except Exception as e:
            prod_dictionary['error_message'] = 'catalog file is not valid'
            prod_dictionary['status'] = 1
            print(e.message)
            logger.exception(e.message)

        return  prod_dictionary

    def set_catalog(self, par_dic, scratch_dir='./'):
        print('---------------------------------------------')
        print('set catalog',par_dic['user_catalog'])

        if par_dic['user_catalog'] is not None:
            user_catalog_file=par_dic['user_catalog']

            self.set_par('user_catalog', build_user_catalog(user_catalog_file))
            #print('==> selecetd catalog')
            #print(user_catalog.table)
            print('==> user catalog done')

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
                #for ra, dec, name in zip(user_catalog.ra, user_catalog.dec, user_catalog.name):
                #    print(name,ra,dec)

            #from cdci_data_analysis.analysis.catalog import BasicCatalog

            #file_path = Path(scratch_dir, 'query_catalog.fits')
            #print('using catalog', file_path)
            #user_catalog = BasicCatalog.from_fits_file(file_path)

            #print('catalog_length', user_catalog.length)
            #self.set_par('user_catalog', user_catalog)
            #print('catalog_selected_objects', catalog_selected_objects)

            #user_catalog.select_IDs(catalog_selected_objects)
            #print('catalog selected\n', user_catalog.table)
            #print('catalog_length', user_catalog.length)
        print('---------------------------------------------')


def build_user_catalog(user_catalog_file):
    return BasicCatalog.from_fits_file(user_catalog_file)


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