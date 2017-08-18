

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


import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt


import mpld3
from mpld3 import plugins

import  numpy as np
from astropy.io import  fits as pf

def draw_fig(image_array,image_header,catalog=None,plot=False):
    from astropy import wcs
    from astropy.wcs import WCS

    from astropy import units as u
    import astropy.coordinates as coord


    fig, (ax) = plt.subplots(1, 1, figsize=(4, 3), subplot_kw={'projection': WCS(image_header)})
    im = ax.imshow(image_array, origin='lower', zorder=1, interpolation='none', aspect='equal')

    if catalog is not None:


        lon = coord.Angle(catalog['RA_FIN'] * u.deg)
        lat = coord.Angle(catalog['DEC_FIN'] * u.deg)

        w = wcs.WCS(image_header)
        pixcrd = w.wcs_world2pix(np.column_stack((lon, lat)), 1)

        ax.plot(pixcrd[:, 0], pixcrd[:, 1], 'o', mfc='none')
        for ID in xrange(catalog.size):
            ax.annotate('%s' % catalog[ID]['NAME'], xy=(pixcrd[:, 0][ID], pixcrd[:, 1][ID]),color='white')

        ax.set_xlabel('RA')
        ax.set_ylabel('DEC')


    fig.colorbar(im, ax=ax)
    if plot==True:
        plt.show()

    plugins.connect(fig, plugins.MousePosition(fontsize=14))

    return mpld3.fig_to_dict(fig)

def draw_spectrum(spectrum,dummy=False):


    rmf=pf.open('rmf_62bands.fits')
    src_spectrum=spectrum[8].data
    E_min=rmf[3].data['E_min']
    E_max=rmf[3].data['E_max']

    msk=src_spectrum['RATE']>0.

    y=src_spectrum['RATE']/(E_max-E_min)

    x = (E_max + E_min)
    dx = np.log10(np.e) * (E_max - E_min) / x
    x = np.log10(x)

    dy=src_spectrum['STAT_ERR']/(E_max-E_min)
    dy=np.log10(np.e)*dy/y
    y=np.log10(y)

    fig, ax = plt.subplots(figsize=(4, 2.8))

    ax.set_xlabel('log(E)  keV')
    ax.set_ylabel('log(counts/s/keV)')

    ax.errorbar(x[msk], y[msk], yerr=dy[msk]*0.5,xerr=dx[msk]*0.5, fmt='o')
    #print (x,y,dy)
    plugins.connect(fig, plugins.MousePosition(fontsize=14))

    return mpld3.fig_to_dict(fig)


def draw_dummy(dummy=True):




    fig, ax = plt.subplots(figsize=(4, 3))

    if dummy == True:
        x = np.linspace(-2, 2, 200)
        y = x[:, None]
        image = np.zeros((200, 200, 4))

        image[:, :, 0] = np.exp(- (x - 1) ** 2 - (y) ** 2)
        image[:, :, 1] = np.exp(- (x + 0.71) ** 2 - (y - 0.71) ** 2)
        image[:, :, 2] = np.exp(- (x + 0.71) ** 2 - (y + 0.71) ** 2)
        image[:, :, 3] = np.exp(-0.25 * (x ** 2 + y ** 2))


    im = ax.imshow(image,origin='lower', zorder=1, interpolation='none',aspect='equal')
    fig.colorbar(im, ax=ax)


    plugins.connect(fig, plugins.MousePosition(fontsize=14))
    
    return mpld3.fig_to_dict(fig)


