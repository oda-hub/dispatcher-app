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



class Job(object):

    def __init__(self,work_dir,server_url,server_port,callback_handle,file_mame='job_monitor.json',job_id=None,session_id=None,status='unaccessible'):
        self.monitor={}
        self.callback_handle=callback_handle
        self.server_url=server_url
        self.server_port=server_port
        self.file_mame=file_mame
        self.work_dir=work_dir
        self.job_id=job_id
        self.session_id=session_id
        self.status=status
        self.update_monitor()

    def update_monitor(self):
        self.monitor['job_id']=self.job_id
        self.monitor['session_id'] = self.session_id
        self.monitor['status']=self.status


    def _set_file_name(self,work_dir):
        return self.work_dir + '/' + self.file_mame

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


    def get_dataserver_status(self,work_dir):
        f_path = self._set_file_name(work_dir)
        #print('f_path', f_path)
        try:
            with open(f_path, 'r') as infile:
                self.monitor = json.load(infile, encoding='utf-8')
            #print('JOB MANAGER CHECK-->', self.monitor)
        except Exception as e:
            self.set_unaccessible()

        return  self.monitor

    def write_dataserver_status(self,work_dir,status_dictionary_value=None):
        f_path = self._set_file_name(work_dir)
        # TODO: add chekc of current and on file job_id and session_id

        if status_dictionary_value is None:
            pass
        else:
            self.monitor['status']=status_dictionary_value

        #print('writing job status to job_monitor', self.monitor['status'])
        with open(f_path, 'w') as outfile:
            my_json_str = json.dumps(self.monitor, encoding='utf-8')
            # if isinstance(my_json_str, str):
            outfile.write(u'%s' % my_json_str)



    def get_call_back_url(self):
        url=u'http://%s:%s/%s?'%(self.server_url,self.server_port,self.callback_handle)
        url+=u'session_id=%s&'%self.session_id
        url += u'job_id=%s&' % self.job_id
        url += u'work_dir=%s&' % self.work_dir
        url += u'file_mame=%s' % self.file_mame

        return url