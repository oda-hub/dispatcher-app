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

# Dependencies
# eg numpy
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


import ddosaclient as dc

# Project
# relative import eg: from .mod import f
from ..analysis.parameters import *
from .osa_dispatcher import OsaQuery, QueryProduct
from ..analysis.queries import LightCurveQuery
from ..analysis.products import LightCurveProduct,QueryProductList
from astropy.io import fits as pf

class IsgriLigthtCurve(LightCurveProduct):
    def __init__(self,name,data,header):


        super(IsgriLigthtCurve, self).__init__(name,data,header)
        #check if you need to copy!



    @classmethod
    def build_from_ddosa_res(cls,name,res,src_name='ciccio'):

        hdu_list = pf.open(res.lightcurve)
        data = None
        header=None
        for hdu in hdu_list:
            if hdu.name == 'ISGR-SRC.-LCR':
                print('name', hdu.header['NAME'])
                if hdu.header['NAME'] == src_name:
                    data = hdu.data
                    header = hdu.header

        spec = cls(name=name, data=data, header=header)

        return spec

def do_lightcurve_from_single_scw(image_E1, image_E2, time_bin_seconds=100, scw=[]):
    """
    builds a spectrum for single scw

    * spectrum is built from image with one_bin mode
    * catalog default catalog is used for the image
    * ddosa selection is applied to build catalog for spectra


    :param image_E1:
    :param image_E2:
    :param scw:
    :return:
    """
    scw_str = str(scw)
    scwsource_module = "ddosa"
    target = "ii_lc_extract"
    modules = ["ddosa", "git://ddosadm"]
    assume = [scwsource_module + '.ScWData(input_scwid="%s")' % scw_str,
              'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=image_E1,
                                                                                                       E2=image_E2),
              'ddosa.ImagingConfig(use_SouFit=0,use_DoPart2=1,use_version="soufit0_p2")',
              'ddosa.CatForLC(use_minsig=3)',
              'ddosa.LCTimeBin(use_time_bin_seconds=100)']

    return QueryProduct(target=target, modules=modules, assume=assume)


def do_lightcurve(E1, E2, scwlist_assumption,src_name, extramodules=[]):
    print('-->lc standard mode from scw_list', scwlist_assumption)
    print('-->src_name', src_name)
    target = "lc_pick"
    modules = ["git://ddosa", "git://ddosadm"] + extramodules
    assume = ['ddosa.LCGroups(input_scwlist=%s)'%scwlist_assumption,
              'ddosa.lc_pick(use_source_names=["%s"])'%src_name,
              'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1, E2=E2),
              'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0_p2",use_DoPart2=1)',
              'ddosa.CatForLC(use_minsig=3)',
              'ddosa.LCTimeBin(use_time_bin_seconds=100)']

    return QueryProduct(target=target, modules=modules, assume=assume)


def do_lc_from_scw_list(E1, E2, src_name,scw_list=None,user_catalog=None):
    print('mosaic standard mode from scw_list', scw_list)
    dic_str = str(scw_list)
    return do_lightcurve(E1, E2, 'ddosa.IDScWList(use_scwid_list=%s)' % dic_str,src_name)


def do_lc_from_time_span(E1, E2, T1, T2, RA, DEC, radius,src_name,user_catalog=None):
    scwlist_assumption = 'rangequery.TimeDirectionScWList(\
                        use_coordinates=dict(RA=%(RA)s,DEC=%(DEC)s,radius=%(radius)s),\
                        use_timespan=dict(T1="%(T1)s",T2="%(T2)s"),\
                        use_max_pointings=3)\
                    ' % (dict(RA=RA, DEC=DEC, radius=radius, T1=T1, T2=T2)),

    return do_lightcurve(E1, E2, scwlist_assumption,src_name, extramodules=['git://rangequery'])



def get_osa_lightcurve(instrument,dump_json=False,use_dicosverer=False,config=None):
    q = OsaQuery(config=config)

    RA = instrument.get_par_by_name('RA').value
    DEC = instrument.get_par_by_name('DEC').value
    radius = instrument.get_par_by_name('radius').value
    scw_list = instrument.get_par_by_name('scw_list').value
    user_catalog = instrument.get_par_by_name('user_catalog').value

    src_name = instrument.get_par_by_name('src_name').value

    if scw_list is not None and scw_list != []:

        if len(instrument.get_par_by_name('scw_list').value) == 1:
            print('-> single scw')

            query_prod = do_lc_from_scw_list(instrument.get_par_by_name('E1_keV').value,
                                         instrument.get_par_by_name('E2_keV').value,
                                         src_name,
                                         scw_list=scw_list,
                                         user_catalog=user_catalog)
        else:
            query_prod = do_lc_from_scw_list(instrument.get_par_by_name('E1_keV').value,
                                         instrument.get_par_by_name('E2_keV').value,
                                         src_name,
                                         scw_list=scw_list,
                                         user_catalog=user_catalog)

    else:
        T1_iso = instrument.get_par_by_name('T1')._astropy_time.isot
        T2_iso = instrument.get_par_by_name('T2')._astropy_time.isot
        query_prod = do_lc_from_time_span(instrument.get_par_by_name('E1_keV').value,
                                                 instrument.get_par_by_name('E2_keV').value,
                                                 T1_iso,
                                                 T2_iso,
                                                 RA,
                                                 DEC,
                                                 radius,
                                                 src_name,
                                                 user_catalog=user_catalog)




    res = q.run_query(query_prod=query_prod)

    print('res', str(res.lightcurve))

    lc = IsgriLigthtCurve.build_from_ddosa_res('isgri_lc',res,src_name=src_name)

    prod_list = QueryProductList(prod_list=[lc])

    return prod_list, None


def get_osa_lightcurve_dummy_products(config):
    from ..analysis.products import LightCurveProduct
    dummy_cache = config.dummy_cache
    query_lc = LightCurveProduct.from_fits_file('%s/query_lc.fits'%dummy_cache, 'isgri_lc', ext=1)

    prod_list = QueryProductList(prod_list=[query_lc])

    return prod_list, None