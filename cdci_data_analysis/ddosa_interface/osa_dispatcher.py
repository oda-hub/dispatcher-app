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
import  ast

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np
import json

# Project
# relative import eg: from .mod import f
import ddosaclient as dc
import  simple_logger
from ..analysis.queries import  *
from ..analysis.job_manager import  Job
import sys
import traceback
import time

import os
from contextlib import contextmanager

# @contextmanager
# def silence_stdout():
#     new_target = open(os.devnull, "w")
#     old_target, sys.stdout = sys.stdout, new_target
#     try:
#         yield new_target
#     finally:
#         sys.stdout = old_target
#
#
#
# def redirect_out(path):
#     #print "Redirecting stdout"
#     sys.stdout.flush() # <--- important when redirecting to files
#     newstdout = os.dup(1)
#     devnull = os.open('%s/SED.log'%path, os.O_CREAT)
#     os.dup2(devnull, 1)
#     os.close(devnull)
#     sys.stdout = os.fdopen(newstdout, 'w')

def view_traceback():
    ex_type, ex, tb = sys.exc_info()
    traceback.print_tb(tb)
    del tb



class QueryProduct(object):

    def __init__(self,target=None,modules=[],assume=[],inject=[]):
        self.target=target
        self.modules=modules
        self.assume=assume
        self.inject=inject


class OsaQuery(object):

    def __init__(self,config=None,use_dicosverer=False):
        print('--> building class OsaQyery')
        simple_logger.log()
        simple_logger.logger.setLevel(logging.ERROR)
        if use_dicosverer == True:
            try:
                c = discover_docker.DDOSAWorkerContainer()

                self.url = c.url
                self.ddcache_root_local = c.ddcache_root
                print("===>managed to read from docker:")



            except Exception as e:
                raise RuntimeError("failed to read from docker", e)

        elif config is not None:
            try:
                # config=ConfigEnv.from_conf_file(config_file)
                self.url = config.dataserver_url
                self.ddcache_root_local = config.dataserver_cache

            except Exception as e:
                #print(e)

                print ("ERROR->")
                e.display()
                raise RuntimeError("failed to use config ", e)

        else:

            raise RuntimeError('either you provide use_dicosverer=True or a config object')

        print("url:", self.url)
        print("ddcache_root:", self.ddcache_root_local)
        print('--> done')


    def test_connection(self):
        print ('--> start test connection')
        #with silence_stdout():

        remote = dc.RemoteDDOSA(self.url,self.ddcache_root_local)

        status=''
        try:
            #with silence_stdout()\
            simple_logger.log()
            simple_logger.logger.setLevel(logging.ERROR)
            product = remote.query(target="ii_spectra_extract",
                                   modules=["ddosa", "git://ddosadm"],
                                   assume=["ddosa" + '.ScWData(input_scwid="035200230010.001")',
                                           'ddosa.ImageBins(use_ebins=[(20,40)],use_version="onebin_20_40")',
                                           'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")'])

        except dc.WorkerException as e:
            content = json.loads(e.content)

            status = content['result']['status']
            print('e=> server connection status', status)

        #status = product['result']['status']
        #print('product=>', product)
        print('--> end test connection')

        return status

    def test_has_input_products(self,instrument):
        print('--> start has input_products')
        RA = instrument.get_par_by_name('RA').value
        DEC = instrument.get_par_by_name('DEC').value
        radius = instrument.get_par_by_name('radius').value
        scw_list = instrument.get_par_by_name('scw_list').value
        #print('scw_list', scw_list)

        if scw_list is not None and scw_list != []:
            return scw_list

        else:
            T1_iso = instrument.get_par_by_name('T1')._astropy_time.isot
            T2_iso = instrument.get_par_by_name('T2')._astropy_time.isot



            target = "ReportScWList"
            modules = ["git://ddosa", "git://ddosadm"] + ['git://rangequery']
            assume = ['rangequery.ReportScWList(\
                                      input_scwlist=\
                                      rangequery.TimeDirectionScWList(\
                                    use_coordinates=dict(RA=%(RA)s,DEC=%(DEC)s,radius=%(radius)s),\
                                    use_timespan=dict(T1="%(T1)s",T2="%(T2)s"),\
                                    use_max_pointings=50))' % (dict(RA=RA, DEC=DEC, radius=radius, T1=T1_iso, T2=T2_iso))]


            remote = dc.RemoteDDOSA(self.url, self.ddcache_root_local)

            try:
                product = remote.query(target=target,modules=modules,assume=assume)
                return product.scwidlist

            except dc.WorkerException as e:
                content = json.loads(e.content)

                status = content['result']['status']
                print('e=> server connection status', status)

                return None


    def test_busy(self,max_trial=25,sleep_s=1):
        print ('--> start test busy')
        simple_logger.log()
        simple_logger.logger.setLevel(logging.ERROR)
        remote = dc.RemoteDDOSA(self.url,self.ddcache_root_local)
        status=''
        time.sleep(sleep_s)
        for i in range(max_trial):
            time.sleep(sleep_s)
            try:
                #with silence_stdout():
                r = remote.poke()
                print('remote poke ok')
                status=''
                break
            except dc.WorkerException as e:

                content=json.loads(e.content)

                status= content['result']['status']
                print('e=>', i, status)

        if status=='busy':
            print ('server is busy')
            raise Exception

        print('--> end test busy')

    def run_query(self,query_prod,job,prompt_delegate=True):
        res = None
        try:
            #redirect_out('./')
            #with silence_stdout():
            simple_logger.logger.setLevel(logging.ERROR)

            if isinstance(job,Job):
                pass
            else:
                raise RuntimeError('job object not passed')


            res= dc.RemoteDDOSA(self.url, self.ddcache_root_local).query(target=query_prod.target,
                                                   modules=query_prod.modules,
                                                   assume=query_prod.assume,
                                                   inject=query_prod.inject,
                                                   prompt_delegate=prompt_delegate,
                                                   callback=job.get_call_back_url())

            print ('url for call_back',job.get_call_back_url())
            print("cached object in", res,res.ddcache_root_local)
            job.set_done()
        except dc.WorkerException as e:

            job.set_done()
            print("ERROR->")
            e.display()
            raise RuntimeWarning('ddosa connection or processing failed',e)

        except dc.AnalysisDelegatedException as e:

            if isinstance(job,Job):
                pass
            else:
                raise RuntimeError('job object not passed')

        return res






