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

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy
import os

# Dependencies
# eg numpy
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


import ddosaclient as dc

# Project
# relative import eg: from .mod import f
import  numpy as np

from ..analysis.parameters import *
from .osa_dispatcher import OsaQuery
from ..analysis.queries import LightCurveQuery
from ..analysis.products import LightCurveProduct,QueryProductList,QueryOutput
from astropy.io import fits as pf

class IsgriLigthtCurve(LightCurveProduct):
    def __init__(self,name,file_name,data,header,prod_prefix=None,out_dir=None,src_name=None):


        super(IsgriLigthtCurve, self).__init__(name,
                                               data,
                                               header,
                                               file_name=file_name,
                                               name_prefix=prod_prefix,
                                               file_dir=out_dir,
                                               src_name=src_name)



    @classmethod
    def build_from_ddosa_res(cls,
                             name,
                             file_name,
                             res,
                             src_name='ciccio',
                             prod_prefix = None,
                             out_dir = None):

        hdu_list = pf.open(res.lightcurve)
        data = None
        header=None

        for hdu in hdu_list:
            if hdu.name == 'ISGR-SRC.-LCR':
                print('name', hdu.header['NAME'])
                if hdu.header['NAME'] == src_name:
                    data = hdu.data
                    header = hdu.header

            lc = cls(name=name, data=data, header=header,file_name=file_name,out_dir=out_dir,prod_prefix=prod_prefix,src_name=src_name)

        return lc


class OsaLightCurveQuery(LightCurveQuery):

    def __init__(self, name):

        super(OsaLightCurveQuery, self).__init__(name)

    def get_products_method(self, instrument, job, prompt_delegate, dump_json=False, use_dicosverer=False, config=None,
                            out_dir=None, prod_prefix=None):

        q = self.get_osa_query(instrument, config=config)

        src_name,res = q.run_query(job=job, prompt_delegate=prompt_delegate)

        if job.status != 'done':
            prod_list = QueryProductList(prod_list=[], job=job)
            return prod_list
        else:
            return self.build_poduct_list(job, res, out_dir, prod_prefix,src_name)

    def get_osa_query_pars(self, instrument):

        RA = instrument.get_par_by_name('RA').value
        DEC = instrument.get_par_by_name('DEC').value
        radius = instrument.get_par_by_name('radius').value
        scw_list = instrument.get_par_by_name('scw_list').value
        user_catalog = instrument.get_par_by_name('user_catalog').value
        use_max_pointings = instrument.max_pointings
        src_name = instrument.get_par_by_name('src_name').value
        delta_t = instrument.get_par_by_name('time_bin')._astropy_time_delta.sec
        print('delta_t is sec', delta_t)

        extramodules = []
        if scw_list is None or scw_list != []:
            T1_iso = instrument.get_par_by_name('T1')._astropy_time.isot
            T2_iso = instrument.get_par_by_name('T2')._astropy_time.isot
        else:
            T1_iso = None
            T2_iso = None
            extramodules = ['git://rangequery']

        scwlist_assumption = self.get_scwlist_assumption(T1_iso, T2_iso, RA, DEC, radius, use_max_pointings,delta_t)
        instr_user_catalog = self.get_instr_catalog(user_catalog)

        target, modules, assume = self.set_instr_dictionaries(extramodules, scwlist_assumption,src_name,delta_t)

        inject = []

        if instr_user_catalog is not None:
            print('user_catalog', user_catalog.ra)

            cat = ['SourceCatalog',
                   {
                       "catalog": [
                           {
                               "RA": float(ra.deg),
                               "DEC": float(dec.deg),
                               "NAME": name,
                           }
                           for ra, dec, name in zip(user_catalog.ra, user_catalog.dec, user_catalog.name)
                       ],
                       "version": "v2",  # catalog id here; good if user-understandable, but can be computed internally
                       "autoversion": True,  # this will complement the version with some hash of the data
                       # consider the above version now to be the version of the version generation
                   }
                   ]
            inject.append(cat)

            modules.append("git://gencat")

        return OsaQuery(target=target, modules=modules, assume=assume, inject=inject),src_name

    def process_product_method(self, instrument, job, prod_list):
        query_lc = prod_list.get_prod_by_name('isgri_lc')

        prod_dictionary = {}
        # if query_lc is not None and query_lc.data is not None:

        query_lc.write(overwrite=True)

        query_out = QueryOutput()

        if query_lc.data is not None:
            html_fig = query_lc.get_html_draw()
            query_out.prod_dictionary['image'] = html_fig
            query_out.prod_dictionary['file_name'] = str(query_lc.file_path.name)
            query_out.prod_dictionary['session_id'] = job.session_id
            query_out.prod_dictionary['job_id'] = job.job_id
            query_out.prod_dictionary['download_file_name'] = 'light_curve.fits.gz'
            query_out.prod_dictionary['prod_process_maessage'] = ''
        else:
            query_out.prod_dictionary['image'] = None
            query_out.prod_dictionary['file_name'] = ''
            query_out.prod_dictionary['session_id'] = ''
            query_out.prod_dictionary['job_id'] = ''
            query_out.prod_dictionary['download_file_name'] = ''
            query_out.prod_dictionary[
                'prod_process_maessage'] = 'no light curve produced for name %s', query_lc.src_name
        print('--> send prog')

        return query_out

class IsgriLightCurveQuery(OsaLightCurveQuery):
    def __init__(self,name ):
        super(IsgriLightCurveQuery, self).__init__(name)





    def build_product_list(self,job,res,out_dir,prod_prefix,src_name):

        lc = IsgriLigthtCurve.build_from_ddosa_res('isgri_lc', 'query_lc.fits',
                                                   res,
                                                   src_name=src_name,
                                                   prod_prefix=prod_prefix,
                                                   out_dir=out_dir)

        # print('spectrum_list',spectrum_list)
        prod_list = QueryProductList(prod_list=lc,job=job)


        return prod_list

    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2,src_name,delta_t):
        print('-->lc standard mode from scw_list', scwlist_assumption)
        print('-->src_name', src_name)
        target = "lc_pick"

        modules = ["git://ddosa", "git://ddosadm"]
        if extramodules is not None:
            modules += extramodules

        assume = ['ddosa.LCGroups(input_scwlist=%s)' % scwlist_assumption,
                  'ddosa.lc_pick(use_source_names=["%s"])' % src_name,
                  'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,
                                                                                                           E2=E2),
                  'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0_p2",use_DoPart2=1)',
                  'ddosa.CatForLC(use_minsig=3)',
                  'ddosa.LCTimeBin(use_time_bin_seconds=%f)' % delta_t]

        return target, modules, assume

    def get_dummy_products(self, instrument, config, out_dir='./'):
        src_name = instrument.get_par_by_name('src_name').value
        from ..analysis.products import LightCurveProduct
        dummy_cache = config.dummy_cache
        delta_t = instrument.get_par_by_name('time_bin')._astropy_time_delta.sec
        print('delta_t is sec', delta_t)
        query_lc = LightCurveProduct.from_fits_file(inf_file='%s/query_lc.fits' % dummy_cache,
                                                    out_file_name='query_lc.fits',
                                                    prod_name='isgri_lc',
                                                    ext=1,
                                                    file_dir=out_dir)
        print('name', query_lc.header['NAME'])

        if src_name is not None:
            if query_lc.header['NAME'] != src_name:
                query_lc.data = None

        prod_list = QueryProductList(prod_list=[query_lc])

        return prod_list




