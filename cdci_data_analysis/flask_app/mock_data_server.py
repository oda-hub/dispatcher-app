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
import yaml


import threading
import time
import subprocess, os

class MyThread(threading.Thread):
    def run(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        cmd = ["python", dir_path + '/'+'mock_worker.py N=%d job_id=%s session_id=%s scratch_dir=%s'%(self.N,self.job_id,self.session_id,self.scratch_dir)]
        print("command:", " ".join(cmd))
        os.spawnl(os.P_NOWAIT, cmd )

        # #mimic call_back
        # job_status={}
        #
        # job_status['status']='submitted'
        # job_status['fraction']=0.0
        # f_path=self.scratch_dir+'/'+'status.yml'
        # for i in range(self.N):
        #     print('i',i)
        #     time.sleep(1)
        #     job_status['fraction']=float(i)/(self.N-1)
        #     with open(f_path, 'w') as outfile:
        #         yaml.dump(job_status, outfile, default_flow_style=False)
        # #
        # job_status['status'] = 'done'
        # with open(f_path, 'w') as outfile:
        #     yaml.dump(job_status, outfile, default_flow_style=False)

        return






def spawn(N,job_id,session_id,scratch_dir):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    cmd = ["python", dir_path + '/' + 'mock_worker.py',"%s %s %s %d & " % (job_id, session_id, scratch_dir,N) ]
    print("command:", " ".join(cmd))

    os.system(" ".join(cmd))
    #subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
def mock_request(session_id,job_id,scratch_dir):

    #thread = MyThread()
    #thread.daemon = True
    #thread.session_id=session_id
    #thread.job_id=job_id
    #thread.scratch_dir=scratch_dir
    #thread.N=20
    #thread.start()

    spawn(20, job_id, session_id, scratch_dir)
    job_status = {}
    job_status['status'] = 'submitted'
    job_status['fraction'] = ''
    return job_status


def mock_chek_job_status(job_id,session_id,scratch_dir):
    f_path = scratch_dir + '/' + 'status.yml'
    print ('f_path',f_path)
    try:
        with open(f_path, 'r') as outfile:
            job_status=yaml.load(outfile)
        print ('-->',job_status)
    except:
        job_status = {}
        job_status['status'] = 'unacessible'
        job_status['fraction'] = ''

    return job_status