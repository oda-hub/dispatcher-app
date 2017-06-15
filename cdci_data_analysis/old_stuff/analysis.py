

from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)



__author__ = "Andrea Tramacere"

import numpy as np


from astropy.io import  fits as pf


from ..ddosa_interface.osa_image_dispatcher import get_osa_image




# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f

class AnalysisProduct(object):
    pass

class Image(AnalysisProduct):
    pass

class LightCurve(AnalysisProduct):
    pass

class Spectrum(AnalysisProduct):
    pass


class TimeRange(object):
    pass

class EnergyRange(object):
    pass





class AnalysisConf(object):

    def __init__(self,request):

        self.E1 = request.form['E1']
        self.E2 = request.form['E2']
        self.T1=None
        self.T2=None
        self.time_format=None

        if request.form['time_format'] == 'iso':
            self.T1 = request.form['t_start']
            self.T2 = request.form['t_stop']
            self.time_range='from_time'
            print(self.T1, self.T2)

        elif request.form['time_format'] == 'mjd':
            self.T1 = request.form['mjd_start']
            self.T2 = request.form['mjd_stop']
            self.time_range = 'from_time'
            print(self.T1, self.T2)

        elif request.form['time_format'] == 'scw_list':
            self.scw_list = [x.strip() for x in request.form['scw_list'].split(',')]
            self.time_range = 'from_scw_list'
            print(self.scw_list)

        else:
            raise  RuntimeError('wrong time format')




def get_image_from_query(request,osa_conf):
    #analysis_conf = AnalysisConf(request)
    analysis_conf=None
    if request.form['image_type'] == 'Real':
        image_paht,exception = get_osa_image(analysis_conf, config=osa_conf, use_dicosverer=False, )
        if exception is None:
            try:
                image = pf.getdata(image_paht, ext=4)
            except:
                raise RuntimeError('file path not valid', image_paht)
        else:
            return 'Error: %s'%exception

        return image
    else:
        N=20
        x = np.linspace(-2, 2, N)
        y = x[:, None]
        image = np.zeros((N, N , 4))

        image[:, :, 0] = np.exp(- (x - 1) ** 2 - (y) ** 2)
        image[:, :, 1] = np.exp(- (x + 0.71) ** 2 - (y - 0.71) ** 2)
        image[:, :, 2] = np.exp(- (x + 0.71) ** 2 - (y + 0.71) ** 2)
        image[:, :, 3] = np.exp(-0.25 * (x ** 2 + y ** 2))

    return image