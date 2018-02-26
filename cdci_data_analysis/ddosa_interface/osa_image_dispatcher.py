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

import  os

# Project
# relative import eg: from .mod import f
from ..analysis.queries import ImageQuery
from ..analysis.parameters import *
from .osa_catalog import  OsaIsgriCatalog,OsaJemxCatalog
from .osa_dispatcher import    OsaQuery
from ..analysis.products import QueryProductList,CatalogProduct,ImageProduct,QueryOutput
from ..analysis.job_manager import Job
from ..web_display import draw_fig
from astropy.io import  fits as pf




class OsaImageProduct(ImageProduct):

    def __init__(self,name,file_name,skyima,out_dir=None,prod_prefix=None):
        header = skyima.header
        data = skyima.data
        super(OsaImageProduct, self).__init__(name,data=data,header=header,name_prefix=prod_prefix,file_dir=out_dir,file_name=file_name)
        #check if you need to copy!





    @classmethod
    def build_from_ddosa_skyima(cls,name,file_name,skyima,out_dir=None,prod_prefix=None):
        skyima = pf.open(skyima)
        return  cls(name,skyima=skyima[4],out_dir=out_dir,prod_prefix=prod_prefix,file_name=file_name)





class OsaMosaicQuery(ImageQuery):

    def __init__(self,name):

        super(OsaMosaicQuery, self).__init__(name)

    def get_products_method(self,instrument,job,prompt_delegate,dump_json=False,use_dicosverer=False,config=None,out_dir=None,prod_prefix=None):



        q=self.get_osa_query(instrument,config=config)


        res = q.run_query( job=job, prompt_delegate=prompt_delegate)

        if job.status != 'done':
            prod_list = QueryProductList(prod_list=[], job=job)
            return prod_list
        else:
           return self.build_poduct_list(job,res,out_dir,prod_prefix)

    def process_product_method(self, instrument, job, prod_list):

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

    def get_osa_query_pars(self, instrument):

        # time_range_type = instrument.get_par_by_name('time_group_selector').value
        RA = instrument.get_par_by_name('RA').value
        DEC = instrument.get_par_by_name('DEC').value
        radius = instrument.get_par_by_name('radius').value
        scw_list = instrument.get_par_by_name('scw_list').value
        user_catalog = instrument.get_par_by_name('user_catalog').value
        use_max_pointings = instrument.max_pointings

        extramodules = []
        if scw_list is None or scw_list != []:
            T1_iso = instrument.get_par_by_name('T1')._astropy_time.isot
            T2_iso = instrument.get_par_by_name('T2')._astropy_time.isot
        else:
            T1_iso = None
            T2_iso = None
            extramodules = ['git://rangequery']

        scwlist_assumption = self.get_scwlist_assumption(T1_iso, T2_iso, RA, DEC, radius, use_max_pointings)
        instr_user_catalog = self.get_instr_catalog(user_catalog)

        target, modules, assume = self.set_instr_dictionaries(extramodules, scwlist_assumption)

        inject = []

        if instr_user_catalog is not None:
            cat = ['SourceCatalog',
                   {
                       "catalog": [
                           {
                               "RA": float(ra.deg),
                               "DEC": float(dec.deg),
                               "NAME": str(name),
                           }
                           for ra, dec, name in zip(instr_user_catalog.ra, instr_user_catalog.dec, v.name)
                       ],
                       "version": "v1",  # catalog id here; good if user-understandable, but can be computed internally
                       "autoversion": True,
                   }
                   ]

            extramodules.append("git://gencat")
            inject.append(cat)

        return OsaQuery(target=target, modules=modules, assume=assume, inject=inject)


    def get_scwlist_assumption(self,scw_list,T1,T2,RA,DEC,radius,use_max_pointings):
        if scw_list is not None and scw_list != []:
            scwlist_assumption = 'rangequery.TimeDirectionScWList(\
                                    use_coordinates=dict(RA=%(RA)s,DEC=%(DEC)s,radius=%(radius)s),\
                                    use_timespan=dict(T1="%(T1)s",T2="%(T2)s"),\
                                    use_max_pointings=%(use_max_pointings)d)\
                                ' % (dict(RA=RA, DEC=DEC, radius=radius, T1=T1, T2=T2, use_max_pointings=use_max_pointings))

        else:
           scwlist_assumption=str(scw_list)

        return scwlist_assumption


    def get_instr_catalog(self, user_catalog):
        raise RuntimeError('Must be specified for each instrument')


    def set_instr_dictionaries(self,catalog,):
        raise RuntimeError('Must be specified for each instrument')













class JemxMosaicQuery(OsaMosaicQuery):
    def __init__(self,name ):
        super(JemxMosaicQuery, self).__init__(name)


    def get_dummy_products(self, instrument, config=None, **kwargs):
        pass


    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2):
        target = "mosaic_jemx"
        modules = ["git://ddosa", "git://ddosadm", "git://ddjemx", 'git://rangequery'] + extramodules
        # assume = ['ddjemx.JMXImageGroups(input_scwlist=%s)' % scwlist_assumption,
        #          'ddjemx.JEnergyBins(use_bins=[(%(E1)s,%(E2)s)])' % dict(E1=E1, E2=E2)]

        # target = "mosaic_jemx"
        # modules = ["git://ddosa", "git://ddosadm", "git://ddjemx", "git://rangequery"] + extramodules
        assume = ['ddjemx.JMXScWImageList(input_scwlist=%s)' % scwlist_assumption,
                  'ddjemx.JEnergyBins(use_bins=[(%(E1)s,%(E2)s)])' % dict(E1=E1, E2=E2),
                  'ddjemx.JEMX(use_num=2)']

        return target, modules, assume



    def build_product_list(self, job, res, out_dir, prod_prefix):
        image = OsaImageProduct.build_from_ddosa_skyima('mosaic_image', 'jemx_query_mosaic.fits', res.skyima,
                                                        out_dir=out_dir, prod_prefix=prod_prefix)
        osa_catalog = CatalogProduct('mosaic_catalog', catalog=OsaJemxCatalog.build_from_ddosa_srclres(res.srclres),
                                     file_name='query_catalog.fits', name_prefix=prod_prefix, file_dir=out_dir)

        prod_list = QueryProductList(prod_list=[image, osa_catalog], job=job)

        return prod_list


class IsgriMosaicQuery(OsaMosaicQuery):
    def __init__(self,name ):
        super(IsgriMosaicQuery, self).__init__(name)





    def build_product_list(self,job,res,out_dir,prod_prefix):
        image = OsaImageProduct.build_from_ddosa_skyima('mosaic_image', 'isgri_query_mosaic.fits', res.skyima,
                                                            out_dir=out_dir, prod_prefix=prod_prefix)
        osa_catalog = CatalogProduct('mosaic_catalog',
                                         catalog=OsaIsgriCatalog.build_from_ddosa_srclres(res.srclres),
                                         file_name='query_catalog.fits', name_prefix=prod_prefix, file_dir=out_dir)

        prod_list = QueryProductList(prod_list=[image, osa_catalog], job=job)


        return prod_list

    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2):
        target = "mosaic_ii_skyimage"
        modules = ["git://ddosa", "git://ddosadm"] + extramodules
        assume = ['ddosa.ImageGroups(input_scwlist=%s)' % scwlist_assumption,
                  'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,
                                                                                                           E2=E2),
                  'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")', ]


        return target,modules,assume

    def get_dummy_products(self, instrument, config, out_dir='./'):
        from ..analysis.products import ImageProduct
        from ..analysis.catalog import BasicCatalog
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

