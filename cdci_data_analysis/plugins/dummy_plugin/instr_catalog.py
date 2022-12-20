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

from cdci_data_analysis.analysis.io_helper import FitsFile
from cdci_data_analysis.analysis.catalog import BasicCatalog



class MyInstrCatalog(BasicCatalog):

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

        super(MyInstrCatalog, self).__init__(src_names,
                 lon,
                 lat,
                 significance,
                 unit=unit,
                 frame=frame,)



        self.add_column(data=NEW_SOURCE, name='NEW_SOURCE')
        self.add_column(data=ISGRI_FLAG, name='ISGRI_FLAG', dtype=np.int32)
        self.add_column(data=FLAG, name='FLAG', dtype=np.int32)
        self.add_column(data=ERR_RAD, name='ERR_RAD', dtype=np.float64)

    @classmethod
    def build_from_dict_list(cls, distlist):
        frame = "FK5"

        get_key_column = lambda key, default=None: [de.get(key, default) for de in distlist]

        print(get_key_column('name'), cls)

        return cls(get_key_column('name'),
                   get_key_column('ra'),
                   get_key_column('dec'),
                   significance=get_key_column('DETSIG', 0),
                   frame="fk5",
                   ISGRI_FLAG=get_key_column("ISGRI_FLAG", 1),
                   NEW_SOURCE=get_key_column("NEW_SOURCE", 0),
                   FLAG=get_key_column("FLAG", 1),
                   ERR_RAD=get_key_column('err_rad', 0.01))

    @classmethod
    def build_from_ddosa_srclres(cls, srclres,prod_prefix=None):
        #catalog = pf.open(srclres)[1]
        catalog=FitsFile(srclres).open()[1]

        print ('cat file',srclres)
        frame = catalog.header['RADECSYS'].lower()
        catalog=catalog.data
        return cls( [n.strip() for n in catalog['NAME']],
                    catalog['RA_FIN'],
                    catalog['DEC_FIN'],
                    significance=catalog['DETSIG'],
                    frame=frame,
                    NEW_SOURCE=catalog['NEW_SOURCE'],
                    ISGRI_FLAG=catalog['ISGRI_FLAG'],
                    FLAG=catalog['FLAG'],
                    ERR_RAD=catalog['ERR_RAD'] )





