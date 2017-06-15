
   


from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

from cdci_data_analysis import  conf_dir

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




#----------------------------------------
# launch
#----------------------------------------


class ConfigEnv(object):

	def __init__(self,local_cache,ddcache_root,data_server_url,data_server_port,dispatcher_url,dispatcher_port):
		self.local_cache=os.path.abspath(local_cache)
		self.ddcache_root=ddcache_root
		self.data_server_url=data_server_url
		self.data_server_port=data_server_port
		self.dispatcher_url=dispatcher_url
		self.dispatcher_port = dispatcher_port

		self.dataserver_url = 'http://%s:%d' % (self.data_server_url, self.data_server_port)
		self.dataserver_cache = '%s/%s' % (self.local_cache, self.ddcache_root)

	@classmethod
	def from_conf_file(cls,conf_file_path):
		if conf_file_path is None:
			conf_file_path = conf_dir+'/conf_env.yml'

		with open(conf_file_path, 'r') as ymlfile:
			cfg = yaml.load(ymlfile)


		return ConfigEnv(local_cache=cfg['local_cache'],
						 ddcache_root=cfg['ddcache_root'],
						 data_server_url=cfg['data_server_url'],
						 data_server_port=cfg['data_server_port'],
						 dispatcher_url=cfg['dispatcher_url'],
						 dispatcher_port=cfg['dispatcher_port'])


