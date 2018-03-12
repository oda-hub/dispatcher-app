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

import json
# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f

from ..analysis.io_helper import FilePath

class Job(object):

    def __init__(self,work_dir,server_url,server_port,callback_handle,file_name='job_monitor.json',job_id=None,session_id=None,status='unaccessible'):
        self.monitor={}
        self.callback_handle=callback_handle
        self.server_url=server_url
        self.server_port=server_port
        self._set_file_path(file_name=file_name,work_dir=work_dir)
        #print ("ciccio",self._file_path.path, self._file_path.name,self._file_path.dir_name)
        self.job_id=job_id
        self.session_id=session_id
        self.status=status
        self.update_monitor()

    def update_monitor(self):
        self.monitor['job_id']=self.job_id
        self.monitor['session_id'] = self.session_id
        self.monitor['status']=self.status


    def _set_file_path(self,file_name,work_dir):
        self._file_path=FilePath(file_dir=work_dir,file_name=file_name)

    @property
    def file_path(self):
        return self._file_path.path

    @property
    def file_name(self):
        return self._file_path.name

    @property
    def dir_name(self):
        return self._file_path.dir_name

    def _set_status(self,job_status):
        self.monitor['status']=job_status
        self.status=job_status

    def set_submitted(self):
        self._set_status('submitted')

    def set_done(self):
        self._set_status('done')

    def set_failed(self):
        self._set_status('failed')

    def set_unaccessible(self):
        self._set_status('unaccessible')


    def get_dataserver_status(self,):
        # TODO: combine all files

        try:
            with open(self.file_path, 'r') as infile:
                #print("=====> reading  from ", self.file_path)
                self.monitor = json.load(infile, encoding='utf-8')
            #print('JOB MANAGER CHECK-->', self.monitor)
        except Exception as e:
            self.set_unaccessible()

        return  self.monitor

    def write_dataserver_status(self,status_dictionary_value=None,full_dict=None):
        # TODO: write to specific name coming for call_back

        if status_dictionary_value is None:
            pass
        else:
            self.monitor['status']=status_dictionary_value

        #print('writing job status to job_monitor', self.monitor['status'])
        if full_dict is not None:
            self.monitor['full_report_dict']=full_dict

        with open(self.file_path, 'w')  as outfile:
            #print ("=====> writing to ",self.file_path)
            my_json_str = json.dumps(self.monitor, encoding='utf-8')
            # if isinstance(my_json_str, str):
            outfile.write(u'%s' % my_json_str)



    def get_call_back_url(self):
        url=u'http://%s:%s/%s?'%(self.server_url,self.server_port,self.callback_handle)
        url+=u'session_id=%s&'%self.session_id
        url += u'job_id=%s&' % self.job_id
        url += u'work_dir=%s&' % self.dir_name
        url += u'file_mame=%s' % self.file_name
        #print ('-------------> url call back',url)
        return url