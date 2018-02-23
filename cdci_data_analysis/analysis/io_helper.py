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

import sys
import traceback

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy
import  os
from pathlib import Path

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f



class FilePath(object):
    def __init__(self,file_name='',file_dir=u'./',name_prefix=None):
        if name_prefix is not None:
            file_name=name_prefix+'_'+file_name

        if file_dir is None:
            file_dir=u'./'

        if file_name is None:
            file_name=u''

        self._set_file_path(file_dir,file_name)


    def __repr__(self):
        return self._file_path.as_posix()

    def _set_file_path(self,file_dir,file_name):
        self._file_path = Path(file_dir, file_name)

    @property
    def path(self):
        return self._file_path.as_posix()

    @property
    def name(self):
        return self._file_path.name

    @property
    def dir_name(self):
        if self._file_path.is_dir():
            return self.path
        else:
            return self._file_path.parent.as_posix()

    def get_file_path(self,file_name=None,file_dir=None):
        if file_name is  None and file_dir is None:
            file_path=self.path
        elif file_name is  None and file_dir is not None:
            file_path= FilePath(file_dir=file_dir, file_name=self.self.file_path.name).path
        elif  file_name is not  None and file_dir is  None:
            file_path =  self._file_path.with_name(file_name).as_posix()
        elif file_name is not  None and file_dir is not  None:
            self._file_path = Path(file_dir, file_name)
        else:
            file_path= self.path

        return file_path


    def is_dir(self):
        return self._file_path.is_dir()


    def exists(self):
        return self._file_path.exists()

    def mkdir(self,):
        if  self._file_path.exists()==True:
            print('!warning already existing dir', self.path)
            if self._file_path.is_dir():
                pass
            else:
                raise RuntimeError('!Error, path',self.path,'exists and is not a directory')


        else:
            try:
                self._file_path.mkdir()
            except Exception as e:
                raise RuntimeError('!Error error ',e,'in creating dir', self.path)


    def rmdir(self):
        self._file_path.rmdir()



    def joinpath(self,name):
        self._file_path.joinpath(name)

    def clean_dir(self):
        pass


def view_traceback():
    ex_type, ex, tb = sys.exc_info()
    print('tb =====>')
    traceback.print_tb(tb)
    print('   <=====')
    del tb