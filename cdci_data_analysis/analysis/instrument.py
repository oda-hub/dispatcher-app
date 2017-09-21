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


from pathlib import Path

import  logging

logger = logging.getLogger(__name__)

import  numpy as np

from cdci_data_analysis.analysis.queries import _check_is_base_query

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
                 product_queries_list=None):

        #name
        self.name=instr_name

        #src query
        self.src_query=src_query


        #Instrument specific
        self.instrumet_query=instrumet_query



        self.product_queries_list=product_queries_list

        self._queries_list=[self.src_query,self.instrumet_query]



        if product_queries_list is not None and product_queries_list !=[]:
            self._queries_list.extend(product_queries_list)

        _check_is_base_query(self._queries_list)



    def _check_names(self):
        pass

    def set_pars_from_dic(self,par_dic):
        print(par_dic.keys())
        for _query in self._queries_list:
            for par in _query._parameters_list:
                par.set_from_form(par_dic)
                #par_name=par.name
                #units_name=par.units_name
                #v=None
                #u=None
                #if par_name  in par_dic.keys():
                #    v=par_dic[par_name]
                #if units_name in  par_dic.keys():
                #    if units_name is not None:
                #        u=par_dic[units_name]
                #print('setting par:', par_name,'to val=',v,'and units',units_name,'to',u)
                #if u is not None:
                #    par.units=u
                #par.value=v


        #for p, v in par_dic.items():
        #    print('set from form', p, v)
        #    self.set_par(p,v)
        #    print('--')

    def set_par(self,par_name,value):
        p=self.get_par_by_name(par_name)
        p.value=value




    def get_query_by_name(self,prod_name):
        p=None
        for _query in self._queries_list:
            if prod_name == _query.name:
                p  =  _query

        if p is None:
            raise Warning('parameter', prod_name, 'not found')

        return p

    def run_query(self,query_name,config=None,out_dir=None,query_type='Real',**kwargs):
        return self.get_query_by_name(query_name).run_query(self,out_dir,query_type=query_type,config=config)

    def get_query_products(self, query_name, config=None,out_dir=None):
        return self.get_query_by_name(query_name).get_products(self, config=config,out_dir=out_dir)

    def get_query_dummy_products(self, query_name, config=None,out_dir=None,**kwargs):

        return self.get_query_by_name(query_name).get_dummy_products(self, config=config,out_dir=out_dir,**kwargs)

    def get_html_draw(self, prod_name, image,image_header,catalog=None):

        return self.get_query_by_name(prod_name).get_html_draw( image,image_header,catalog=catalog)

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

    def set_catalog(self, par_dic, scratch_dir='./'):
        if 'catalog_selected_objects' in par_dic.keys():

            catalog_selected_objects = np.array(par_dic['catalog_selected_objects'].split(','), dtype=np.int)
        else:
            catalog_selected_objects = None

        if catalog_selected_objects is not None:
            from cdci_data_analysis.analysis.catalog import BasicCatalog

            file_path = Path(scratch_dir, 'query_catalog.fits')
            print('using catalog', file_path)
            user_catalog = BasicCatalog.from_fits_file(file_path)

            print('catalog_length', user_catalog.length)
            self.set_par('user_catalog', user_catalog)
            print('catalog_selected_objects', catalog_selected_objects)

            user_catalog.select_IDs(catalog_selected_objects)
            print('catalog selected\n', user_catalog.table)
            print('catalog_length', user_catalog.length)
