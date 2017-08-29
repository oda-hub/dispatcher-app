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
from astropy.coordinates import SkyCoord
from astropy.table import Table,Column
from astropy.io  import fits as pf




class BasicCatalog(object):
    def __init__(self,src_names,lon,lat,significance,unit='deg',frame='FK5',_selected=None):

        self.selected = np.ones(len(src_names), dtype=np.bool)

        if _selected is not None:
            self.selected = False
            self.selected[_selected] = True

        self._sc = SkyCoord(lon,lat,frame=frame, unit=unit)

        self.lat_name, self.lon_name=self.get_coord_names(self.sc)



        self._table = Table([src_names, significance, lon, lat], names=['src_names', 'significance', self.lon_name, self.lat_name])


        self.table.meta['frame']=frame
        self.table.meta['coord_unit'] = unit
        self.table.meta['lon_name'] = self.lon_name
        self.table.meta['lat_name'] = self.lat_name


    def select_all(self):
        self.selected[::]=True

    def unselect_all(self):
        self.selected[::]=False

    def get_coord_names(self,sc):
        inv_map = {v: k for k, v in sc.representation_component_names.items()}

        _lat_name = inv_map['lat']
        _lon_name = inv_map['lon']

        return _lat_name,_lon_name


    @property
    def table(self):
        return self._table[self.selected]


    @property
    def sc(self):
        return self._sc[self.selected]

    @property
    def length(self):
        return self.table.as_array().shape[0]

    @property
    def ra(self):
        return self.sc.fk5.ra

    @property
    def dec(self):
        return self.sc.fk5.dec

    @property
    def l(self):
        return self.sc.galactic.l

    @property
    def b(self):
        return self.sc.galactic.b

    @property
    def name(self):
        return self.table['src_names']

    @property
    def significance(self):
        return self.table['significance']

    @property
    def lat(self):
        return self.table[ self.lat_name]

    @property
    def lon(self):
        return self.table[ self.lon_name]

    def add_column(self,data=None,name=None,dtype=None):

        if data is None:
            data=np.zeros(self.length)
        self.table.add_column(Column(data=data,name=name,dtype=dtype))

    def get_dictionary(self):
        return dict(columns_list=[self.table[name].tolist() for name in self.table.colnames], column_names=self.table.colnames,column_descr=self.table.dtype.descr)


    def write(self,name,format='fits',overwrite=True):
        self.table.write(name,format=format,overwrite=overwrite)

    @classmethod
    def from_fits_file(cls,file_name):
        return cls.from_table(Table.read(file_name,format='fits'))

    @classmethod
    def from_table(cls,table):
        try:
            src_names=table['src_names']
            significance=table['significance']
            frame = table.meta['frame']
            lon=table[table.meta['lon_name']]
            lat =table[table.meta['lat_name']]
            unit = table.meta['coord_unit']

            cat= cls(src_names,lon,lat,frame,significance,unit=unit,frame=frame)
        except:
            raise RuntimeError('Table in fits file is not valid to build Catalog')

        return  cat


