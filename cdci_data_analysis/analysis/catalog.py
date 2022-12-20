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
import  decorator
import  numpy as np
from astropy.coordinates import SkyCoord
from astropy.table import Table,Column
from astropy.io  import fits as pf
from json_tricks import numpy_encode,dumps

@decorator.decorator
def _selector(func,arr,mask):


    return func(arr[mask])


class BasicCatalog(object):
    def __init__(self,src_names,lon,lat,significance,unit='deg',frame='FK5',_selected=None,_table=None):

        self.selected = np.ones(len(src_names), dtype=np.bool)

        if _selected is not None:
            self.selected = False
            self.selected[_selected] = True

        self._sc = SkyCoord(lon,lat,frame=frame, unit=unit)

        self.lat_name, self.lon_name=self.get_coord_names(self.sc)

        meta={'FRAME':frame}
        meta['COORD_UNIT']=unit
        meta['LON_NAME']=self.lon_name
        meta['LAT_NAME']=self.lat_name

        if _table is None:
            self._table = Table([np.arange(len(src_names)),src_names, significance, lon, lat], names=['meta_ID','src_names', 'significance', self.lon_name, self.lat_name],meta=meta,masked=True)
        else:
            self._table=Table(_table.as_array(),names=_table.colnames,meta=meta,masked=True)

    def select_IDs(self,ids):
        self.unselect_all()
        self.selected[ids]=True


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
        return self._table.as_array().shape[0]

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
            data=np.zeros(self.table.as_array().shape[0])
        self._table.add_column(Column(data=data,name=name,dtype=dtype))

    def get_dictionary(self ):


        column_lists=[self.table[name].tolist() for name in self.table.colnames]
        for ID,_col in enumerate(column_lists):
            column_lists[ID] = [x if str(x)!='nan' else None for x in _col]

        return dict(cat_frame=self.table.meta['FRAME'],
                    cat_coord_units=self.table.meta['COORD_UNIT'],
                    cat_column_list=column_lists,
                    cat_column_names=self.table.colnames,
                    cat_column_descr=self.table.dtype.descr,
                    cat_lat_name=self.lat_name,
                    cat_lon_name=self.lon_name)



    def write_ds9_region(self,name,overwrite=True):
        ra=self.sc.fk5.ra
        dec=self.sc.fk5.dec
        src_names=self.name

        with open(name,'w') as f:
            for r,d,src_name in zip(ra,dec,src_names):

                s=u'''fk5; point %f %f #point = x  text = {%s} \n'''%(r.deg,d.deg,src_name)
                f.write(s)


    #
    #user_catalog = BasicCatalog(src_names, lon, lat, significance, _table=t, unit=unit, frame=frame)

    def decode(self,enc_table):
        pass


    def encode(self,):
        _table=numpy_encode(self.table.as_array())
        _meta=dumps(self.table.meta)

        return dumps(_table,_meta)


    def write(self,name,format='fits',overwrite=True):
        self._table.write(name,format=format,overwrite=overwrite)

    @classmethod
    def from_ecsv_file(cls, file_name):
        return cls.from_table(Table.read(file_name, format='ascii.ecsv'))


    @classmethod
    def from_fits_file(cls,file_name):
        return cls.from_table(Table.read(file_name,format='fits'))

    @classmethod
    def from_file(cls,file_name):
        print(f"Loading catalog from file {file_name}")
        format_list=['ascii.ecsv','fits']
        cat=None
        for f in format_list:
            try:
                cat= cls.from_table(Table.read(file_name,format=f))
            except:
                pass

        if cat is None:
            print("Catalog format not valid")
            raise RuntimeError('file format for catalog not valid')
        return cat

    @classmethod
    def from_table(cls,table):
        try:
            src_names=table['src_names']
            significance=table['significance']
            frame = table.meta['FRAME']
            lon=table[table.meta['LON_NAME']]
            lat =table[table.meta['LAT_NAME']]
            unit = table.meta['COORD_UNIT']
            cat= cls(src_names,lon,lat,significance,_table=table,unit=unit,frame=frame)
        except:
            print(f"Error while parsing catalog from table: \n{table}\n{table.keys()}\n{table.meta.keys()}\n")
            raise RuntimeError('Table in fits file is not valid to build Catalog')

        return  cat


