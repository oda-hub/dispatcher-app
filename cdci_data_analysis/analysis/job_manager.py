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
import glob

import json
import  os

import logging

from urllib.parse import urlencode

logger = logging.getLogger(__name__)

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

    def __init__(self,
                 instrument_name,
                 work_dir,
                 server_url,
                 server_port,
                 callback_handle,
                 file_name='job_monitor.json',
                 job_id=None,
                 session_id=None,
                 status='unaccessible',
                 status_kw_name='action',
                 aliased=False):

        #if aliased is False:
        #
        self.work_dir=work_dir

        #else:
        #    self.work_dir = work_dir +'_aliased'

        self.aliased=aliased
        self.status_kw_name=status_kw_name
        self.instrument_name=instrument_name
        self.monitor={}
        self.callback_handle=callback_handle
        self.server_url=server_url
        self.server_port=server_port
        self._set_file_path(file_name=file_name,work_dir=work_dir)

        #self.job_id=job_id
        #self.session_id=session_id
        #self.status=status
        self.update_monitor(status,session_id,job_id)
        self._allowed_job_status_values_=self.get_allowed_job_status_values()

    @staticmethod
    def get_allowed_job_status_values():
        return ['done', 'failed', 'progress', 'submitted', 'ready', 'unknown', 'unaccessible','aliased', 'post-processing']

    def update_monitor(self,status,session_id,job_id):
        self.monitor['job_id']=job_id
        self.monitor['session_id'] = session_id
        self.monitor['status']=status


    def _set_file_path(self,file_name,work_dir):
        self._file_path=FilePath(file_dir=work_dir,file_name=file_name)

    @property
    def job_id(self):
        return self.monitor['job_id']

    @property
    def status(self):
        return self.monitor['status']

    @status.setter
    def status(self,s):
        self._set_status(s)

    @property
    def session_id(self):
        return self.monitor['session_id']




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
        if job_status in self._allowed_job_status_values_:
            self.monitor['status']=job_status
            #self.status=job_status
        else:
            raise RuntimeError('job_status', job_status, ' in QueryOutput is not allowed',
                               self._allowed_job_status_values_)

    def set_progress(self):
        self._set_status('progress')

    def set_submitted(self):
        self._set_status('submitted')

    def set_done(self):
        self._set_status('done')

    def set_failed(self):
        self._set_status('failed')

    def set_unaccessible(self):
        self._set_status('unaccessible')

    def set_ready(self):
        self._set_status('ready')

    def get_status(self):
        return self.monitor['status']


    def updated_dataserver_monitor(self,):
        # TODO: combine all files

        try:
            with open(self.file_path, 'r') as infile:
                #print("=====> reading  from ", self.file_path)
                self.monitor = json.load(infile,)

            logger.info('JOB MANAGER CHECK-->', self.monitor)
        except Exception as e:
            logger.warning("no current job state: %s", e)
            self.set_unaccessible()

        return self.monitor

    def write_dataserver_status(self, status_dictionary_value=None, full_dict=None):
        # TODO: write to specific name coming for call_back

        if status_dictionary_value is None:
            pass
        else:
            self.monitor['status'] = status_dictionary_value

        #print('writing job status to job_monitor', self.monitor['status'])
        if full_dict is not None:
            self.monitor['full_report_dict'] = full_dict

        with open(self.file_path, 'w') as outfile:
            my_json_str = json.dumps(self.monitor)
            outfile.write(u'%s' % my_json_str)



    def get_call_back_url(self):
        if self.server_port is None:
            url = f'{self.server_url}/{self.callback_handle}'
        else:
            url = f'http://{self.server_url}:{self.server_port}/{self.callback_handle}'

        # TODO: also pass token, to use it later to send email in callback
        url += "?" + urlencode({ k:getattr(self, k) for k in [
                "session_id", "job_id", "work_dir", "file_name", "instrument_name"
            ]})

        url += '&progressing'

        logger.debug("callback url: %s", url)

        return url


class OsaJob(Job):
    def __init__(self,
                 instrument_name,
                 work_dir,
                 server_url,
                 server_port,
                 callback_handle,
                 file_name='job_monitor.json',
                 job_id=None,
                 session_id=None,
                 status='unaccessible',
                 status_kw_name='action',
                 par_dic=None,
                 aliased=False):

        file_id=None
        file_message=None

        if par_dic is not None:
            if 'node_id' in par_dic.keys():
                #print('node_id', par_dic['node_id'])
                file_id=par_dic['node_id']
            else:
                print('No! node_id')

            if 'message' in par_dic.keys():
                file_message=par_dic['message']

        file_flag=''

        if  file_id is not None:
            file_flag += '_%s'%file_id

        if file_message is not None:
            file_flag += '_%s' % file_message.replace(' ','_')

        if file_flag !='':
            file_name = 'job_monitor%s_.json' %file_flag



        super(OsaJob, self).__init__(instrument_name,
                                  work_dir,
                                  server_url,
                                  server_port,
                                  callback_handle,
                                  file_name=file_name,
                                  job_id=job_id,
                                  session_id=session_id,
                                  status=status,
                                  status_kw_name=status_kw_name,
                                  aliased=aliased)

    def updated_dataserver_monitor(self,work_dir=None):
        if work_dir is None:
            work_dir=self.work_dir

        job_files_list = sorted(glob.glob(work_dir + '/job_monitor*.json'), key=os.path.getmtime)
        #print('OSA JOB get data server status form files',job_files_list)
        job_done=False
        job_failed=False
        progress=False
        full_report_dict_list=[]

        n_progress = 0

        for job_file in job_files_list:
            try:
                with open(job_file, 'r') as infile:
                    self.monitor = json.load(infile, encoding='utf-8')
                    #print ('--->for file',job_file,'got',self.monitor['status'])

                    if self.monitor['status'] == 'done':
                        job_done = True
                    elif self.monitor['status'] == 'failed':
                        job_failed = True

                    if 'full_report_dict' in  self.monitor.keys():
                        full_report_dict_list.append(self.monitor['full_report_dict'])

                        if 'progressing' in self.monitor['full_report_dict'].keys():
                            progress = True
                            n_progress += 1

            except Exception as e:
                self.set_unaccessible()

        print(f"found {n_progress} PROGRESS entries in {len(job_files_list)} job_files ({work_dir}/job_monitor*.json)")

        if progress is True:
            self.monitor['status'] = 'progress'

        if job_done == True:
            self.monitor['status'] = 'done'

        if job_failed == True:
            self.monitor['status'] = 'failed'


        self.monitor['full_report_dict_list']=full_report_dict_list
        print('\033[32mfinal status', self.monitor['status'], '\033[0m')
        return  self.monitor



def job_factory(instrument_name, scratch_dir, server_url, dispatcher_port, session_id, job_id, par_dic, aliased=False):
    osa_list = ['jemx', 'isgri', 'empty-async']

    if instrument_name in osa_list:
        j = OsaJob(
             instrument_name=instrument_name,
             work_dir=scratch_dir,
             server_url=server_url,
             server_port=dispatcher_port,
             callback_handle='call_back',
             session_id=session_id,
             job_id=job_id,
             par_dic=par_dic,
             aliased=aliased)
    else:
        j = Job(
             instrument_name=instrument_name,
             work_dir=scratch_dir,
             server_url=server_url,
             server_port=dispatcher_port,
             callback_handle='call_back',
             session_id=session_id,
             job_id=job_id,
             aliased=aliased)

    return j
