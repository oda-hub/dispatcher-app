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



# Project
# relative import eg: from .mod import f
from ..analysis.parameters import *
from .osa_dispatcher import    OsaQuery,QueryProduct
from ..analysis.queries import SpectrumQuery
from ..web_display import draw_spectrum
from ..analysis.products import SpectrumProduct,QueryProductList
from astropy.io import  fits as pf


class IsgriSpectrumProduct(SpectrumProduct):

    def __init__(self,name,data,header, rmf_file=None, arf_file=None):



        super(IsgriSpectrumProduct, self).__init__(name,data,header,in_rmf_file=rmf_file,in_arf_file=arf_file)
        #check if you need to copy!



    @classmethod
    def build_from_ddosa_res(cls,name,res,src_name='ciccio'):

        data = None
        header=None
        print('src_name->', src_name)


        for source_name, spec_attr, rmf_attr, arf_attr in res.extracted_sources:
            if src_name is not None:
                print('-->', source_name, src_name)
                if source_name == src_name:
                    spectrum = pf.open(getattr(res, spec_attr))[1]
                    arf_filename= getattr(res, arf_attr)
                    rmf_filename = getattr(res, rmf_attr)
                    data=spectrum.data
                    header=spectrum.header

        spec= cls(name=name,data=data,header=header,rmf_file=rmf_filename,arf_file=arf_filename)

        spec.set_arf_file(arf_kw='ANCRFILE',out_arf_file='arf.fits')
        spec.set_rmf_file(rmf_kw='RESPFILE',out_rmf_file='rmf.fits')

        return spec

# def do_spectrum_from_single_scw(E1,E2,scw):
#     """
#     builds a spectrum for single scw
#
#     * spectrum is built from image with one_bin mode
#     * catalog default catalog is used for the image
#     * ddosa selection is applied to build catalog for spectra
#
#
#     :param E1:
#     :param E2:
#     :param scw:
#     :return:
#     """
#     scw_str = str(scw)
#     scwsource_module = "ddosa"
#     target = "ii_spectra_extract"
#     modules = ["ddosa", "git://ddosadm"]
#     assume = [scwsource_module + '.ScWData(input_scwid="%s")'%scw_str,
#              'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")'%dict(E1=E1,E2=E2),
#              'ddosa.ImagingConfig(use_SouFit=0,use_DoPart2=1,use_version="soufit0_p2")',
#              'ddosa.CatForSpectraFromImaging(use_minsig=3)',]


def do_spectrum_from_scw_list(E1,E2,scw_list=["035200230010.001","035200240010.001"],user_catalog=None):
    """
     builds a spectrum for list of scw

    * spectrum is built from image with one_bin mode
    * catalog default catalog is used for the image
    * ddosa selection is applied to build catalog for spectra

    :param E1:
    :param E2:
    :param scw_list:
    :return:
    """
    print('sum spectra from scw_list',scw_list)
    dic_str = str(scw_list)
    target = "ISGRISpectraSum"
    modules = ["ddosa", "git://ddosadm", "git://useresponse", "git://process_isgri_spectra", "git://rangequery"]

    assume = ['process_isgri_spectra.ScWSpectraList(input_scwlist=ddosa.IDScWList(use_scwid_list=%s))' % dic_str,
              'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1, E2=E2),
              'process_isgri_spectra.ISGRISpectraSum(use_extract_all=True)',
              'ddosa.ImagingConfig(use_SouFit=0,use_DoPart2=1,use_version="soufit0_p2")',
              'ddosa.CatForSpectraFromImaging(use_minsig=3)',
              ]

    #print(assume)

    return do_spectrum(target, modules, assume, user_catalog=user_catalog)


def do_spectrum_from_time_span(E1,E2,T1,T2,RA,DEC,radius,user_catalog=None):
    """
     builds a spectrum for a time span

     logic is different from do_spectrum_from_scw_list, we provide postion to selecet scw_list

    :param E1:
    :param E2:
    :param T1:
    :param T2:
    :param position:
    :return:
    """
    target="ISGRISpectraSum"
    modules = ["ddosa", "git://ddosadm", "git://useresponse", "git://process_isgri_spectra", "git://rangequery"]
    assume = ['process_isgri_spectra.ScWSpectraList(\
                         input_scwlist=\
                         rangequery.TimeDirectionScWList(\
                          use_coordinates=dict(RA=%(RA)s,DEC=%(DEC)s,radius=%(radius)s),\
                          use_timespan=dict(T1="%(T1)s",T2="%(T2)s"),\
                          use_max_pointings=50 \
                          )\
                      )\
                  '%(dict(RA=RA,DEC=DEC,radius=radius,T1=T1,T2=T2)),
              'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),
              'process_isgri_spectra.ISGRISpectraSum(use_extract_all=True)',
              'ddosa.ImagingConfig(use_SouFit=0,use_DoPart2=1,use_version="soufit0_p2")',
              'ddosa.CatForSpectraFromImaging(use_minsig=3)',
              ]

    return  do_spectrum(target,modules,assume,user_catalog=user_catalog)



def do_spectrum(target,modules,assume,user_catalog=None):
    inject=[]
    if user_catalog is not None:
        print ('user_catalog',user_catalog.ra)

        cat = ['SourceCatalog',
               {
                   "catalog": [
                       {
                           "RA": float(ra.deg),
                           "DEC": float(dec.deg),
                           "NAME": name,
                       }
                       for ra,dec,name in zip(user_catalog.ra,user_catalog.dec,user_catalog.name)
                   ],
                   "version": "v2", # catalog id here; good if user-understandable, but can be computed internally
                   "autoversion": True, # this will complement the version with some hash of the data
                                      # consider the above version now to be the version of the version generation
               }
               ]
        inject.append(cat)

        modules.append("git://gencat")
#        assume.append("ddosa.ii_spectra_extract(input_cat=gencat.CatForSpectra)")

    return QueryProduct(target=target, modules=modules, assume=assume,inject=inject)


def get_osa_spectrum(instrument,dump_json=False,use_dicosverer=False,config=None):

    q=OsaQuery(config=config)




    RA = instrument.get_par_by_name('RA').value
    DEC = instrument.get_par_by_name('DEC').value
    radius = instrument.get_par_by_name('radius').value
    scw_list = instrument.get_par_by_name('scw_list').value
    user_catalog = instrument.get_par_by_name('user_catalog').value

    src_name = instrument.get_par_by_name('src_name').value

    if scw_list is not None and scw_list != []:

        if len(instrument.get_par_by_name('scw_list').value) == 1:
            print('-> single scw')
            query_prod = do_spectrum_from_scw_list(instrument.get_par_by_name('E1_keV').value,
                                                   instrument.get_par_by_name('E2_keV').value,
                                                   scw_list=instrument.get_par_by_name('scw_list').value,
                                                   user_catalog=user_catalog)

        else:
            query_prod = do_spectrum_from_scw_list(instrument.get_par_by_name('E1_keV').value,
                                                   instrument.get_par_by_name('E2_keV').value,
                                                   scw_list=instrument.get_par_by_name('scw_list').value,
                                                   user_catalog=user_catalog)

    else:
        T1_iso = instrument.get_par_by_name('T1')._astropy_time.isot
        T2_iso = instrument.get_par_by_name('T2')._astropy_time.isot
        query_prod = do_spectrum_from_time_span( instrument.get_par_by_name('E1_keV').value,
                                                 instrument.get_par_by_name('E2_keV').value,
                                                 T1_iso,
                                                 T2_iso,
                                                 RA,
                                                 DEC,
                                                 radius,
                                                 user_catalog=user_catalog)

    res = q.run_query(query_prod=query_prod)

    #print ('==> res',res.source_results)
    spectrum=IsgriSpectrumProduct.build_from_ddosa_res('isgri_spectrum',res,src_name)

    prod_list = QueryProductList(prod_list=[spectrum])


    return prod_list, None

def get_osa_spectrum_dummy_products(instrument,config):
    from ..analysis.products import SpectrumProduct
    dummy_cache = config.dummy_cache
    query_spectrum = SpectrumProduct.from_fits_file('%s/query_spectrum.fits'%dummy_cache, 'isgri_spectrum', ext=1)
    query_spectrum.set_arf_file(arf_kw='ANCRFILE', out_arf_file='arf.fits',in_arf_file='%s/arf.fits'%dummy_cache)
    query_spectrum.set_rmf_file(rmf_kw='RESPFILE', out_rmf_file='rmf.fits',in_rmf_file='%s/rmf.fits'%dummy_cache)
    prod_list = QueryProductList(prod_list=[query_spectrum])

    return prod_list, None

