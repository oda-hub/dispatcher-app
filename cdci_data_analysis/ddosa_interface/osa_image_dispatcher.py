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
from ..analysis.queries import ImageQuery
from ..analysis.parameters import *
from .osa_catalog import  OsaCatalog
from .osa_dispatcher import    OsaQuery,QueryProduct
from ..analysis.products import QueryProductList,CatalogProduct,ImageProduct

from ..web_display import draw_fig
from astropy.io import  fits as pf




class IsgriImageProduct(ImageProduct):

    def __init__(self,name,skyima):
        header = skyima.header
        data = skyima.data
        super(IsgriImageProduct, self).__init__(name,data=data,header=header,parameters_list=None)
        #check if you need to copy!



    def write(self,name,overwrite=True):
        pf.writeto(name,self.data,header=self.header,overwrite=overwrite)



    @classmethod
    def build_from_ddosa_skyima(cls,name,skyima):
        skyima = pf.open(skyima)
        return  cls(name,skyima=skyima[4])


def do_image_from_single_scw(E1,E2,scw):


    scw_str = str(scw)
    scwsource_module = "ddosa"
    target = "ii_skyimage"
    modules = ["ddosa", "git://ddosadm"]
    assume = [scwsource_module +'.ScWData(input_scwid="%s")'%scw_str,
              'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),
              'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")']
    return QueryProduct(target=target, modules=modules, assume=assume)





def do_mosaic(E1,E2,scwlist_assumption,extramodules=[],user_catalog=None):

    if user_catalog is not None:
        print ('user_catalog',user_catalog.ra)

        cat = ['SourceCatalog',
               {
                   "catalog": [
                       {
                           "RA": ra,
                           "DEC": dec,
                           "NAME": name,
                       }
                       for ra,dec,name in zip(user_catalog.ra,user_catalog.dec,user_catalog.name)
                   ],
                   "version": "v1" # catalog id here; good if user-understandable, but can be computed internally
               }
               ]
    else:
        cat=None

    print('mosaic standard mode from scw_list', scwlist_assumption)

    target="mosaic_ii_skyimage"
    modules=["git://ddosa", "git://ddosadm"]+extramodules
    assume=['ddosa.ImageGroups(input_scwlist=%s)'%scwlist_assumption,
           'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),
           'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")']


    return  QueryProduct(target=target,modules=modules,assume=assume,inject=[cat])


def do_mosaic_from_scw_list(E1,E2,user_catalog=None,scw_list=["035200230010.001","035200240010.001"]):
    print('mosaic standard mode from scw_list', scw_list)
    dic_str=str(scw_list)
    return do_mosaic(E1,E2,'ddosa.IDScWList(use_scwid_list=%s)'%dic_str,user_catalog=user_catalog)

def do_mosaic_from_time_span(E1,E2,T1,T2,RA,DEC,radius,user_catalog=None):
    scwlist_assumption='rangequery.TimeDirectionScWList(\
                        use_coordinates=dict(RA=%(RA)s,DEC=%(DEC)s,radius=%(radius)s),\
                        use_timespan=dict(T1="%(T1)s",T2="%(T2)s"),\
                        use_max_pointings=3)\
                    '%(dict(RA=RA,DEC=DEC,radius=radius,T1=T1,T2=T2)),

    return do_mosaic(E1,E2,scwlist_assumption,extramodules=['git://rangequery'],user_catalog=user_catalog)




def get_osa_image_products(instrument,dump_json=False,use_dicosverer=False,config=None):

    q=OsaQuery(config=config)

    time_range_type = instrument.get_par_by_name('time_group_selector').value
    RA=instrument.get_par_by_name('RA').value
    DEC=instrument.get_par_by_name('DEC').value
    radius=instrument.get_par_by_name('radius').value
    scw_list=instrument.get_par_by_name('scw_list').value
    user_catalog=instrument.get_par_by_name('user_catalog').value

    print('scw_list',scw_list)

    if scw_list is not None and scw_list!=[]:

        if len(instrument.get_par_by_name('scw_list').value)==1:
            print('-> single scw')
            query_prod = do_mosaic_from_scw_list(instrument.get_par_by_name('E1_keV').value,
                                                 instrument.get_par_by_name('E2_keV').value,
                                                 scw_list=instrument.get_par_by_name('scw_list').value,
                                                 user_catalog=user_catalog)

        else:
            query_prod = do_mosaic_from_scw_list(instrument.get_par_by_name('E1_keV').value,
                                                 instrument.get_par_by_name('E2_keV').value,
                                                 scw_list=instrument.get_par_by_name('scw_list').value,
                                                 user_catalog=user_catalog)

    elif time_range_type == 'time_range_iso':
        query_prod = do_mosaic_from_time_span(instrument.get_par_by_name('E1_keV').value,
                                              instrument.get_par_by_name('E2_keV').value,
                                              instrument.get_par_by_name('T1_iso').value,
                                              instrument.get_par_by_name('T2_iso').value,
                                              RA,
                                              DEC,
                                              radius,
                                              user_catalog=user_catalog)

    else:
        raise RuntimeError('wrong time format')


    osa_catalog=None
    image=None

    res=q.run_query(query_prod=query_prod)

    image=IsgriImageProduct.build_from_ddosa_skyima('isgri_mosaic',res.skyima)
    osa_catalog=CatalogProduct('mosaic_catalog',catalog=OsaCatalog.build_from_ddosa_srclres(res.srclres))

    prod_list=QueryProductList(prod_list=[image,osa_catalog])

    return prod_list,None



