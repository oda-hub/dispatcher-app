

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

def draw_fig(image_array,dummy=False):


    fig, ax = plt.subplots(figsize=(4, 3))

    im = ax.imshow(image_array,origin='lower', zorder=1, interpolation='none',aspect='equal')
    fig.colorbar(im, ax=ax)


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


