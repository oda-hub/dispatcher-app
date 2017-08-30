

from __future__ import absolute_import, division, print_function

__author__ = "Andrea Tramacere"


# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f

import json

from .parameters import *

from astropy import wcs
from astropy.wcs import WCS



import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt


import mpld3
from mpld3 import plugins


class QueryProductList(object):

    def __init__(self,prod_list):
        self._prod_list=prod_list

    @property
    def prod_list(self):
        return  self._prod_list

    def get_prod_by_name(self,name):
        prod=None
        for prod1 in self._prod_list:
            if prod1.name==name:
                prod=prod1
        if prod is None:
            raise  Warning('product',name,'not found')
        return prod

class BaseQueryProduct(object):


    def __init__(self,name):
        self.name=name


    def write(self):
        pass


    def read(self):
        pass



class ImageProduct(BaseQueryProduct):
    def __init__(self,name,data,header,parameters_list,**kwargs):
        self.data=data
        self.header=header
        super(ImageProduct, self).__init__(name, **kwargs)


    def get_html_draw(self, catalog=None,plot=False):


        fig, (ax) = plt.subplots(1, 1, figsize=(4, 3), subplot_kw={'projection': WCS(self.header)})
        im = ax.imshow(self.data, origin='lower', zorder=1, interpolation='none', aspect='equal')

        if catalog is not None:

            lon = catalog.ra
            lat = catalog.dec

            w = wcs.WCS(self.header)
            pixcrd = w.wcs_world2pix(np.column_stack((lon, lat)), 1)
            
            msk=~np.isnan(pixcrd[:, 0])
            ax.plot(pixcrd[:, 0][msk], pixcrd[:, 1][msk], 'o', mfc='none')
            
            for ID in xrange(catalog.length):
                if msk[ID]:
                    #print ('xy',(pixcrd[:, 0][ID], pixcrd[:, 1][ID]))
                    ax.annotate('%s' % catalog.name[ID], xy=(pixcrd[:, 0][ID], pixcrd[:, 1][ID]), color='white')
                            

            ax.set_xlabel('RA')
            ax.set_ylabel('DEC')

        fig.colorbar(im, ax=ax)
        if plot == True:
            plt.show()

        plugins.connect(fig, plugins.MousePosition(fontsize=14))

        return mpld3.fig_to_dict(fig)


class LightCurveProduct(BaseQueryProduct):
    def __init__(self,name, **kwargs):
        super(LightCurveProduct, self).__init__(name, **kwargs)


class SpectrumProduct(BaseQueryProduct):
    def __init__(self, name, **kwargs):
        super(SpectrumProduct, self).__init__(name, **kwargs)




class CatalogProduct(BaseQueryProduct):
    def __init__(self, name,catalog, **kwargs):
        self.catalog=catalog
        super(CatalogProduct, self).__init__(name, **kwargs)


    def write(self,name,overwrite=True,format='fits'):
        return self.catalog.write(name,overwrite=overwrite,format=format)
