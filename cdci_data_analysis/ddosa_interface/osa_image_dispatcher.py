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
from .osa_catalog import  OsaCatalog
from .osa_dispatcher import    OsaQuery,QueryProduct
from ..analysis.products import QueryProductList,CatalogProduct,ImageProduct,QueryOutput

from ..web_display import draw_fig
from astropy.io import  fits as pf




class IsgriImageProduct(ImageProduct):

    def __init__(self,name,file_name,skyima,out_dir=None,prod_prefix=None):
        header = skyima.header
        data = skyima.data
        super(IsgriImageProduct, self).__init__(name,data=data,header=header,name_prefix=prod_prefix,file_dir=out_dir,file_name=file_name)
        #check if you need to copy!





    @classmethod
    def build_from_ddosa_skyima(cls,name,file_name,skyima,out_dir=None,prod_prefix=None):
        skyima = pf.open(skyima)
        return  cls(name,skyima=skyima[4],out_dir=out_dir,prod_prefix=prod_prefix,file_name=file_name)


# def do_image_from_single_scw(E1,E2,scw):
#
#
#     scw_str = str(scw)
#     scwsource_module = "ddosa"
#     target = "ii_skyimage"
#     modules = ["ddosa", "git://ddosadm"]
#     assume = [scwsource_module +'.ScWData(input_scwid="%s")'%scw_str,
#               'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),
#               'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")']
#     return QueryProduct(target=target, modules=modules, assume=assume)





def do_mosaic(instr_name,E1,E2,scwlist_assumption,extramodules=None,user_catalog=None):

    inject=[]
    print ('extramodules',extramodules)
    print ('user_catalog',user_catalog)
    if extramodules is None:
        extramodules=[]

    if user_catalog is not None:
        #print ('user_catalog',user_catalog.ra)

        cat = ['SourceCatalog',
               {
                   "catalog": [
                       {
                           "RA": float(ra.deg),
                           "DEC": float(dec.deg),
                           "NAME": str(name),
                       }
                       for ra,dec,name in zip(user_catalog.ra,user_catalog.dec,user_catalog.name)
                   ],
                   "version": "v1", # catalog id here; good if user-understandable, but can be computed internally
                   "autoversion":True,
               }
               ]

        extramodules.append("git://gencat")
        inject.append(cat)
    print ('extramodules',extramodules)
    print('mosaic standard mode from scw_list', scwlist_assumption)

    if instr_name=='ISGRI':
        target="mosaic_ii_skyimage"
        modules=["git://ddosa", "git://ddosadm"]+extramodules
        assume=['ddosa.ImageGroups(input_scwlist=%s)'%scwlist_assumption,
               'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),
               'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")']

        if user_catalog is not None:
            assume.append("ddosa.mosaic_ii_skyimage(use_ii_NegModels=1)")

    elif  instr_name=='JEMX':
        target = "mosaic_jemx"

        modules = ["git://ddosa", "git://ddosadm", "git://ddjemx", 'git://rangequery'] + extramodules,

        assume = ['ddosa.JMXScWImageList(input_scwlist=%s)' % scwlist_assumption,
                  'ddosa.JEnergyBins(use_bins=[(%(E1)s,%(E2)s)])' % dict(E1=E1, E2=E2)]

        if user_catalog is not None:
            raise RuntimeError("jemx catalog not implemented")
            #assume.append("ddosa.mosaic_ii_skyimage(use_ii_NegModels=1)")

    else:
        # TODO: add allowed_instrument_list in the configuration and check on that before!
        raise RuntimeError('Instrumet %s not implemented'%instr_name)



    return  QueryProduct(target=target,modules=modules,assume=assume,inject=inject)


def do_mosaic_from_scw_list(instr_name,E1,E2,user_catalog=None,scw_list=["035200230010.001","035200240010.001"]):
    print('mosaic standard mode from scw_list', scw_list)
    dic_str=str(scw_list)
    return do_mosaic(instr_name,E1,E2,'ddosa.IDScWList(use_scwid_list=%s)'%dic_str,user_catalog=user_catalog)

def do_mosaic_from_time_span(instr_name,E1,E2,T1,T2,RA,DEC,radius,use_max_pointings,user_catalog=None):
    scwlist_assumption='rangequery.TimeDirectionScWList(\
                        use_coordinates=dict(RA=%(RA)s,DEC=%(DEC)s,radius=%(radius)s),\
                        use_timespan=dict(T1="%(T1)s",T2="%(T2)s"),\
                        use_max_pointings=%(use_max_pointings)d)\
                    '%(dict(RA=RA,DEC=DEC,radius=radius,T1=T1,T2=T2,use_max_pointings=use_max_pointings))

    return do_mosaic(instr_name,E1,E2,scwlist_assumption,user_catalog=user_catalog,extramodules=['git://rangequery'])


def get_osa_image_products(instrument,dump_json=False,use_dicosverer=False,config=None,out_dir=None,prod_prefix=None):

    q=OsaQuery(config=config)

    #time_range_type = instrument.get_par_by_name('time_group_selector').value
    RA=instrument.get_par_by_name('RA').value
    DEC=instrument.get_par_by_name('DEC').value
    radius=instrument.get_par_by_name('radius').value
    scw_list=instrument.get_par_by_name('scw_list').value
    user_catalog=instrument.get_par_by_name('user_catalog').value
    use_max_pointings=instrument.max_pointings

    if scw_list is not None and scw_list!=[]:

        if len(instrument.get_par_by_name('scw_list').value)==1:
            #print('-> single scw')
            query_prod = do_mosaic_from_scw_list(instrument.name,
                                                 instrument.get_par_by_name('E1_keV').value,
                                                 instrument.get_par_by_name('E2_keV').value,
                                                 scw_list=instrument.get_par_by_name('scw_list').value,
                                                 user_catalog=user_catalog)

        else:
            query_prod = do_mosaic_from_scw_list(instrument.name,
                                                 instrument.get_par_by_name('E1_keV').value,
                                                 instrument.get_par_by_name('E2_keV').value,
                                                 scw_list=instrument.get_par_by_name('scw_list').value,
                                                 user_catalog=user_catalog)

    else:
        T1_iso=instrument.get_par_by_name('T1')._astropy_time.isot
        T2_iso=instrument.get_par_by_name('T2')._astropy_time.isot

        query_prod = do_mosaic_from_time_span(instrument.name,
                                              instrument.get_par_by_name('E1_keV').value,
                                              instrument.get_par_by_name('E2_keV').value,
                                              T1_iso,
                                              T2_iso,
                                              RA,
                                              DEC,
                                              radius,
                                              use_max_pointings,
                                              user_catalog=user_catalog)




    #osa_catalog=None
    #image=None

    res=q.run_query(query_prod=query_prod)


    image=IsgriImageProduct.build_from_ddosa_skyima('isgri_mosaic','isgri_query_mosaic.fits',res.skyima,out_dir=out_dir,prod_prefix=prod_prefix)
    osa_catalog=CatalogProduct('mosaic_catalog',catalog=OsaCatalog.build_from_ddosa_srclres(res.srclres),file_name='query_catalog.fits',name_prefix=prod_prefix,file_dir=out_dir)

    prod_list=QueryProductList(prod_list=[image,osa_catalog])

    return prod_list


def get_osa_image_dummy_products(instrument,config,out_dir='./'):
    from ..analysis.products import ImageProduct
    from ..analysis.catalog import BasicCatalog
    dummy_cache=config.dummy_cache

    failed=False
    image=None
    catalog=None


    user_catalog = instrument.get_par_by_name('user_catalog').value


    image = ImageProduct.from_fits_file(in_file='%s/isgri_query_mosaic.fits'%dummy_cache,
                                        out_file_name='isgri_query_mosaic.fits',
                                        prod_name='isgri_mosaic',
                                        ext=0,
                                        file_dir=out_dir)

    catalog = CatalogProduct(name='mosaic_catalog',
                             catalog=BasicCatalog.from_fits_file('%s/query_catalog.fits'%dummy_cache),
                             file_name='query_catalog.fits',
                             file_dir = out_dir)



    if user_catalog is not None:
        print ('setting from user catalog',user_catalog,catalog)
        #print (user_catalog.length,catalog.catalog.length)
        #print('setting from user catalog', user_catalog, catalog)
        #catalog.catalog.selected=user_catalog.selected
        catalog.catalog=user_catalog
        #user_catalog.name='mosaic_catalog'

    prod_list = QueryProductList(prod_list=[image, catalog])
    return prod_list



def process_osa_image_products(instrument,prod_list):

    query_image = prod_list.get_prod_by_name('isgri_mosaic')
    query_catalog = prod_list.get_prod_by_name('mosaic_catalog')
    detection_significance = instrument.get_par_by_name('detection_threshold').value

    if detection_significance is not None:
        #query_catalog.catalog.selected = np.logical_and(
        #    query_catalog.catalog._table['significance'] > float(detection_significance),
        #    query_catalog.catalog.selected)
        query_catalog.catalog.selected= query_catalog.catalog._table['significance'] > float(detection_significance)


    print('--> query was ok')
    # file_path = Path(scratch_dir, 'query_mosaic.fits')
    query_image.write(overwrite=True)
    # file_path = Path(scratch_dir, 'query_catalog.fits')
    query_catalog.write(overwrite=True)

    html_fig = query_image.get_html_draw(catalog=query_catalog.catalog,
                                         vmin=instrument.get_par_by_name('image_scale_min').value,
                                         vmax=instrument.get_par_by_name('image_scale_max').value)

    query_out=QueryOutput()

    query_out.prod_dictionary['image'] = html_fig
    query_out.prod_dictionary['catalog'] = query_catalog.catalog.get_dictionary()
    query_out.prod_dictionary['file_path'] = str(os.path.basename(query_image.file_path.get_file_path()))
    query_out.prod_dictionary['file_name'] = 'image.gz'
    query_out.prod_dictionary['prod_process_maessage'] = ''

    return query_out
