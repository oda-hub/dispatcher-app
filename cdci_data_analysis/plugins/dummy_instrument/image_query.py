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

from cdci_data_analysis.analysis.catalog import BasicCatalog
from cdci_data_analysis.analysis.io_helper import FitsFile
from cdci_data_analysis.analysis.products import ImageProduct, QueryProductList, QueryOutput, CatalogProduct
from cdci_data_analysis.analysis.queries import ImageQuery
from .data_server_dispatcher import DataServerQuery
from .instr_catalog import MyInstrCatalog

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


class MyInstrImageProduct(ImageProduct):

    def __init__(self,name,file_name,skyima,out_dir=None,prod_prefix=None):
        header = skyima.header
        data = skyima.data
        super(MyInstrImageProduct, self).__init__(name,data=data,header=header,name_prefix=prod_prefix,file_dir=out_dir,file_name=file_name)
        #check if you need to copy!





    @classmethod
    def build_from_ddosa_skyima(cls,name,file_name,skyima,out_dir=None,prod_prefix=None):
        #skyima = pf.open(skyima)
        skyima = FitsFile(skyima).open()
        return  cls(name,skyima=skyima[4],out_dir=out_dir,prod_prefix=prod_prefix,file_name=file_name)





class MosaicQuery(ImageQuery):

    def __init__(self,name):

        super(MosaicQuery, self).__init__(name)


    def get_products(self,instrument,job,prompt_delegate,dump_json=False,use_dicosverer=False,config=None,out_dir=None,prod_prefix='query_spectrum'):
        scwlist_assumption, cat, extramodules, inject=DataServerQuery.get_osa_query_base(instrument)
        E1=instrument.get_par_by_name('E1_keV').value
        E2=instrument.get_par_by_name('E2_keV').value
        target, modules, assume=self.set_instr_dictionaries(extramodules,scwlist_assumption,E1,E2)
        q=DataServerQuery(config=config, target=target, modules=modules, assume=assume, inject=inject)

        #import sys
        #print ('ciccio',target,modules,assume,inject)

        res = q.run_query( job=job, prompt_delegate=prompt_delegate)

        if job.status != 'done':
            prod_list = QueryProductList(prod_list=[], job=job)
            return prod_list
        else:
           return self.build_product_list(job,res,out_dir,prod_prefix)


    def process_product(self, instrument, job, prod_list):

        query_image = prod_list.get_prod_by_name('mosaic_image')
        query_catalog = prod_list.get_prod_by_name('mosaic_catalog')
        detection_significance = instrument.get_par_by_name('detection_threshold').value

        if detection_significance is not None:
            query_catalog.catalog.selected = query_catalog.catalog._table['significance'] > float(
                detection_significance)

        print('--> query was ok')
        # file_path = Path(scratch_dir, 'query_mosaic.fits')
        query_image.write(overwrite=True)
        # file_path = Path(scratch_dir, 'query_catalog.fits')
        query_catalog.write(overwrite=True)

        html_fig = query_image.get_html_draw(catalog=query_catalog.catalog,
                                             vmin=instrument.get_par_by_name('image_scale_min').value,
                                             vmax=instrument.get_par_by_name('image_scale_max').value)

        query_out = QueryOutput()

        query_out.prod_dictionary['image'] = html_fig
        query_out.prod_dictionary['catalog'] = query_catalog.catalog.get_dictionary()
        # TODO: use query_image.file_path.path -> DONE AND PASSED
        # print ("########## TESTING TODO: use query_image.file_path.path ", query_image.file_path.path)
        query_out.prod_dictionary['file_name'] = str(query_image.file_path.name)

        query_out.prod_dictionary['session_id'] = job.session_id
        query_out.prod_dictionary['job_id'] = job.job_id

        query_out.prod_dictionary['download_file_name'] = 'image.gz'
        query_out.prod_dictionary['prod_process_maessage'] = ''

        return query_out


    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2):
        raise RuntimeError('Must be specified for each instrument')


class MyInstrMosaicQuery(MosaicQuery):
    def __init__(self,name ):
        super(MyInstrMosaicQuery, self).__init__(name)


    def get_dummy_products(self, instrument, config=None, **kwargs):
        pass


    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2):
        target = "mosaic_jemx"
        modules = ["git://ddosa", "git://ddosadm", "git://ddjemx", 'git://rangequery'] + extramodules

        assume = ['ddjemx.JMXScWImageList(input_scwlist=%s)' % scwlist_assumption,
                  'ddjemx.JEnergyBins(use_bins=[(%(E1)s,%(E2)s)])' % dict(E1=E1, E2=E2),
                  'ddjemx.JEMX(use_num=2)']

        return target, modules, assume

    def build_product_list(self, job, res, out_dir, prod_prefix):

        image = MyInstrMosaicQuery.build_from_ddosa_skyima('mosaic_image', 'jemx_query_mosaic.fits', res.skyima,
                                                        out_dir=out_dir, prod_prefix=prod_prefix)
        osa_catalog = CatalogProduct('mosaic_catalog', catalog=MyInstrCatalog.build_from_ddosa_srclres(res.srclres),
                                     file_name='query_catalog.fits', name_prefix=prod_prefix, file_dir=out_dir)

        prod_list = QueryProductList(prod_list=[image, osa_catalog], job=job)

        return prod_list




    def get_dummy_products(self, instrument, config, out_dir='./'):

        dummy_cache = config.dummy_cache

        failed = False
        image = None
        catalog = None

        user_catalog = instrument.get_par_by_name('user_catalog').value

        image = ImageProduct.from_fits_file(in_file='%s/isgri_query_mosaic.fits' % dummy_cache,
                                            out_file_name='isgri_query_mosaic.fits',
                                            prod_name='mosaic_image',
                                            ext=0,
                                            file_dir=out_dir)

        catalog = CatalogProduct(name='mosaic_catalog',
                                 catalog=BasicCatalog.from_fits_file('%s/query_catalog.fits' % dummy_cache),
                                 file_name='query_catalog.fits',
                                 file_dir=out_dir)

        if user_catalog is not None:
            print('setting from user catalog', user_catalog, catalog)
            catalog.catalog = user_catalog

        prod_list = QueryProductList(prod_list=[image, catalog])
        return prod_list