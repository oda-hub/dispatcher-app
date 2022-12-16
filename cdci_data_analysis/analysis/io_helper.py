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
from astropy.io import fits as pf
from flask import request
from werkzeug.utils import secure_filename
import decorator
# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


@decorator.decorator
def check_exist(func,self,**kwargs):
    if self.file_path.exists()==False:
        raise RuntimeError('file %s',self.file_path.path,'does not exists')
    else:
        return func(self)



class File(object):

    def __init__(self,file_path):

        self.file_path=FilePath(file_path)


    @check_exist
    def read(self):
        pass

    @check_exist
    def write(self):
        pass



class FitsFile(File):
    def __init__(self,file_path):
        super(FitsFile,self).__init__(file_path)

    #@check_exist
    def open(self):
        #print('ciccio r', self.file_path)
        return pf.open(self.file_path.path)
        #print ('ciccio r',r)
        #return r

    #@check_exist
    def writeto(self,out_filename=None, data=None, header=None, output_verify='exception', overwrite=False, checksum=False):
        if out_filename is None:
            out_filename=self.file_path.path

        if data is None:

            pf.open(self.file_path.path).writeto(out_filename,output_verify=output_verify,overwrite=overwrite,checksum=checksum)
        else:
            pf.writeto(out_filename,data,header=header,output_verify=output_verify,overwrite=overwrite,checksum=checksum)

class FilePath(object):
    def __init__(self,file_name='',file_dir=u'./',name_prefix=None):
        if name_prefix is not None and name_prefix !='':
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


    def remove(self):
        return self._file_path.unlink()

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


def upload_file(name, dir):
    if name not in request.files:
        return None
    else:
        file = request.files[name]
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '' or file.filename is None:
            return None

        filename = secure_filename(file.filename)
        file_path = os.path.join(dir, filename)
        file.save(file_path)
        return file_path


def format_size(size_bytes, format_returned='M'):
    size_bytes = float(size_bytes)
    size_kb = size_bytes / 1024
    size_mb = size_kb / 1024
    size_gb = size_mb / 1024
    if format_returned == 'M':
        return "%.2fM" % size_mb
    elif format_returned == 'G':
        return "%.2fG" % size_gb
    else:
        return "%.2fkb" % size_kb