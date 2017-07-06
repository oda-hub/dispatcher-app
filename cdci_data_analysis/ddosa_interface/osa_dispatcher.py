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


class QueryProduct(object):

    def __init__(self,target=None,modules=[],assume=[]):
        self.target=target
        self.modules=modules
        self.assume=assume


class OsaQuery(object):

    def __init__(self,config=None,use_dicosverer=False):

        if use_dicosverer == True:
            try:
                c = discover_docker.DDOSAWorkerContainer()

                self.url = c.url
                self.ddcache_root_local = c.ddcache_root
                print("managed to read from docker:")



            except Exception as e:
                raise RuntimeError("failed to read from docker", e)

        elif config is not None:
            try:
                # config=ConfigEnv.from_conf_file(config_file)
                self.url = config.dataserver_url
                self.ddcache_root_local = config.dataserver_cache

            except Exception as e:
                print(e)
                raise RuntimeError("failed to use config ", e)

        else:

            raise RuntimeError('either you provide use_dicosverer=True or a config object')

        print("url:", self.url)
        print("ddcache_root:",  self.ddcache_root_local)


    def run_query(self,query_prod):

        try:
            res= dc.RemoteDDOSA(self.url, self.ddcache_root_local).query(target=query_prod.target,
                                           modules=query_prod.modules,
                                           assume=query_prod.assume)
            print("cached object in", res,res.ddcache_root_local)
        except dc.WorkerException as e:
            print(e)
            raise RuntimeError('ddosa connection or processing failed',e)

        #sprint('res',res)
        return res

    def get_data(self,res,prod_name,json_file=None):
        data = ast.literal_eval(str(res['data']))

        if json_file is not None:
            with open(json_file, 'wb') as outfile:
                json.dump(data, outfile, sort_keys=True, indent=4, separators=(',', ': '))

            print("jsonifiable data dumped to ",json_file)



        e = None
        prod_path = None
        #print ('product keys',data.keys())
        try:
            v = data[prod_name]

        except Exception as e:
            print("Error ", e)
            prod_path = None

        if e is None:
            prod_path = (res['cached_path'][0].replace("data/ddcache", self.ddcache_root_local) + "/" + v[1]).replace(
                "//",
                "/") + ".gz"


        return data,prod_path,e


