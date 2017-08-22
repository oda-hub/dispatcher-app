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
import  numpy as np

from astropy.io import  fits as pf

from ..analysis.catalog import BasicCatalog



class OsaCatalog(BasicCatalog):

    def __init__(self,
                 src_names,
                 lon,
                 lat,
                 significance,
                 unit='deg',
                 frame='FK5',
                 NEW_SOURCE=None,
                 ISGRI_FLAG=None,
                 FLAG=None,
                 ERR_RAD=None):

        super(OsaCatalog, self).__init__(src_names,
                 lon,
                 lat,
                 significance,
                 unit=unit,
                 frame=frame,)



        self.add_column(data=NEW_SOURCE, name='NEW_SOURCE')
        self.add_column(data=ISGRI_FLAG, name='ISGRI_FLAG', dtype=np.int)
        self.add_column(data=FLAG, name='FLAG', dtype=np.int)
        self.add_column(data=ERR_RAD, name='ERR_RAD', dtype=np.float)

    @classmethod
    def build_from_srclres(cls,srclres):
        catalog = pf.open(srclres)[1]
        frame = catalog.header['RADECSYS'].lower()
        catalog=catalog.data
        return cls( catalog['NAME'],
                    catalog['RA_FIN'],
                    catalog['DEC_FIN'],
                    significance=catalog['DETSIG'],
                    frame=frame,
                    NEW_SOURCE=catalog['NEW_SOURCE'],
                    ISGRI_FLAG=catalog['ISGRI_FLAG'],
                    FLAG=catalog['FLAG'],
                    ERR_RAD=catalog['ERR_RAD'])




