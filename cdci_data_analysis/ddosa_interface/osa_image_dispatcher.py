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
from ..analysis.products import Image
from ..analysis.parameters import *
from .osa_dispatcher import    OsaQuery,QueryProduct


def do_mosaic_from_scw_list(E1,E2,scw_list=["035200230010.001","035200240010.001"]):

    dic_str=str(scw_list)
    target="Mosaic"
    modules=["ddosa", "git://ddosadm", "git://osahk", "git://mosaic"]
    assume=['mosaic.ScWImageList(input_scwlist=ddosa.IDScWList(use_scwid_list=%s))'%dic_str,
           'mosaic.Mosaic(use_pixdivide=4)',
           'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),
           'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")']

    return  QueryProduct(target=target,modules=modules,assume=assume)

def do_mosaic_from_time_span(E1,E2,T1,T2):
    target="Mosaic"
    modules=["ddosa","git://ddosadm","git://osahk","git://mosaic",'git://rangequery']
    assume=['mosaic.ScWImageList(\
                            input_scwlist=\
                                rangequery.TimeDirectionScWList(\
                                    use_coordinates=dict(RA=83,DEC=22,radius=5),\
                                    use_timespan=dict(T1=%(T1)s,T2=%(T2)s)),\
                                    use_max_pointings=3 \
                            )\
                            '%dict(T1=T1,T2=T2),
           'mosaic.Mosaic(use_pixdivide=4)',
           'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),
           'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")']

    return QueryProduct(target=target, modules=modules, assume=assume)






def get_osa_image(analysis_prod,dump_json=False,use_dicosverer=False,config=None):

    q=OsaQuery(config=config)


    if analysis_prod.time_group_selector == 'from_scw_list':

        query_prod=do_mosaic_from_scw_list(analysis_prod.E1, analysis_prod.E2, analysis_prod.scw_list)

    elif analysis_prod.time_group_selector == 'time_range_iso':
        query_prod = do_mosaic_from_time_span( analysis_prod.E1, analysis_prod.E2, analysis_prod.T1, analysis_prod.T2)

    else:
        raise RuntimeError('wrong time format')


    res=q.run_query(query_prod=query_prod)

    data, image_path, e=q.get_data(res,'skiima')

    return image_path,e




def OSA_ISGRI_IMAGE():
        E1_keV = Energy('keV', 'E1', value=20.0)
        E2_keV = Energy('keV', 'E2', value=40.0)

        E_range_keV = ParameterRange(E1_keV, E2_keV, 'E_range')

        t1_iso = Time('iso', 'T1_iso', value='2001-12-11T00:00:00.0')
        t2_iso = Time('iso', 'T2_iso', value='2001-12-11T00:00:00.0')

        t1_mjd = Time('mjd', 'T1_mjd', value=1.0)
        t2_mjd = Time('mjd', 'T2_mjd', value=1.0)



        t_range_iso = ParameterRange(t1_iso, t2_iso,'time_range_iso')
        t_range_mjd = ParameterRange(t1_mjd, t2_mjd,'time_range_mjd')

        scw_list = Time('prod_list', 'scw_list', value='035200230010.001,035200240010.001')



        time_group = ParameterGroup([t_range_iso, t_range_mjd,scw_list],'time_range',selected='scw_list')

        time_group_selector = ParameterGroup.build_selector('time_group_selector')


        E_cut=Energy('keV','E_cut',value=0.1)
        parameters_list = [E_range_keV, time_group,time_group_selector,scw_list,E_cut]





        return Image(parameters_list,get_product_method=get_osa_image)