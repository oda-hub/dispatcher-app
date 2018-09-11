from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

from cdci_data_analysis import conf_dir
from cdci_data_analysis.analysis.io_helper import FilePath
import yaml

import sys
import os

__author__ = "Andrea Tramacere"


# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


# ----------------------------------------
# launch
# ----------------------------------------


class DataServerConf(object):

    def __init__(self, data_server_url, data_server_port, data_server_remote_cache, dispatcher_mnt_point,
                         dummy_cache):
        # dataserver port
        self.data_server_port = data_server_port

        # dataserver url
        self.data_server_url = data_server_url
        if self.data_server_url  is not None and self.data_server_port is not None:
            self.data_server_url = 'http://%s'%(self.data_server_url)
        if self.data_server_url is not None and self.data_server_port is not None:
            self.data_server_url +=':%d' % (self.data_server_port)

        # dummy prods local cache
        self.dummy_cache = dummy_cache

        # path to dataserver cache
        self.data_server_remote_path = data_server_remote_cache

        if dispatcher_mnt_point is not None:
            self.dispatcher_mnt_point = os.path.abspath(dispatcher_mnt_point)
            FilePath(file_dir=self.dispatcher_mnt_point).mkdir()
        else:
            self.dispatcher_mnt_point=None



        if self.dispatcher_mnt_point is not None and self.data_server_remote_path is not None:
            #self.data_server_cache = os.path.join(self.dispatcher_mnt_point, self.data_server_remote_path)
            self.data_server_cache = os.path.join(self.dispatcher_mnt_point, self.data_server_remote_path)
        else:
            self.data_server_cache=None

        print(' --> DataServerConf')
        for v in  vars(self):
            print ('attr:',v,getattr(self,v))

    @classmethod
    def from_conf_dict(cls,conf_dict):

        # dataserver port
        data_server_port = conf_dict['data_server_port']

        # dataserver url
        data_server_url = conf_dict['data_server_url']

        # dummy prods local cache
        dummy_cache = conf_dict['dummy_cache']

        # path to dataserver cache
        data_server_remote_cache = conf_dict['data_server_cache']

        dispatcher_mnt_point = conf_dict['dispatcher_mnt_point']

        return DataServerConf(data_server_url,data_server_port,data_server_remote_cache,dispatcher_mnt_point,dummy_cache)

    @classmethod
    def from_conf_file(cls, conf_file):

        with open(conf_file, 'r') as ymlfile:
            cfg_dict = yaml.load(ymlfile)

        return DataServerConf.from_conf_dict(cfg_dict)


class ConfigEnv(object):
    def __init__(self,
                 cfg_dict):

        print ('--> ConfigEnv')
        self._data_server_conf_dict={}
        print ('keys found in cfg_dict',cfg_dict.keys())

        if 'data_server' in cfg_dict.keys():

            for instr_name in cfg_dict['data_server']:
                self.add_data_server_conf_dict(instr_name,cfg_dict)

                print('--> data server key conf',instr_name,self._data_server_conf_dict[instr_name])

        if 'dispatcher' in cfg_dict.keys():

            disp_dict=cfg_dict['dispatcher']

            self.set_conf_dispatcher(disp_dict['dispatcher_url'],
                                     disp_dict['dispatcher_port'],
                                     disp_dict['sentry_url'],
                                     disp_dict['logstash_host'],
                                     disp_dict['logstash_port']
                                     )

            print('--> dispatcher key conf',disp_dict)

        print('--> _data_server_conf_dict', self._data_server_conf_dict)


    def get_data_server_conf_dict(self,instr_name):
        ds_dict=None
        if instr_name in self._data_server_conf_dict.keys():
            ds_dict=  self._data_server_conf_dict[instr_name]

        print ('ds_dict from get_data_server_conf_dict', ds_dict)
        return ds_dict

    def add_data_server_conf_dict(self,instr_name,_dict):
        self._data_server_conf_dict[instr_name] = _dict
        #self._data_server_conf_dict[instr_name] = DataServerConf.from_conf_dict(data_server_conf_dict)

    def set_conf_dispatcher(self,dispatcher_url,dispatcher_port,sentry_url,logstash_host,logstash_port):
        # Generic to dispatcher
        print(dispatcher_url, dispatcher_port)
        self.dispatcher_url = dispatcher_url
        self.dispatcher_port = dispatcher_port
        self.sentry_url=sentry_url
        self.logstash_host=logstash_host
        self.logstash_port=logstash_port



    def get_data_serve_conf(self,instr_name):
        if instr_name in self.data_server_conf_dict.keys():
            c= self._data_server_conf_dict[instr_name]
        else:
            c=None

        return c

    @classmethod
    def from_conf_file(cls, conf_file_path):
        if conf_file_path is None:
            conf_file_path = conf_dir + '/conf_env.yml'
        print('conf_file_path', conf_file_path)
        with open(conf_file_path, 'r') as ymlfile:
            print('conf_file_path', ymlfile )
            cfg_dict = yaml.load(ymlfile)
        #print('cfg_dict',cfg_dict)
        print ('CICCIO')
        return ConfigEnv(cfg_dict)
