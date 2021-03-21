from __future__ import absolute_import, division, print_function
import traceback

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

from cdci_data_analysis import conf_dir
from cdci_data_analysis.analysis.io_helper import FilePath
import yaml

import sys
import os
import logging

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

logger = logging.getLogger("conf")

class DataServerConf:

    def __init__(self, required_keys=None, allowed_keys=None, **kwargs):
        if required_keys is None:
            #temporary hardcode to preserve interface
            required_keys = ['data_server_url', 'dummy_cache']

        if allowed_keys is None:
            #temporary hardcode to preserve interface
            allowed_optional_keys = ['optional_key']
        else:
            allowed_optional_keys = [x for x in allowed_keys if x not in required_keys]

        obsolete_keys = ['data_server_port', 'data_server_host']

        conf = kwargs.copy()

        logger.info("building config from %s", conf)


        try:
            self.data_server_url = conf.pop('data_server_url')
            required_keys.remove('data_server_url')
        except KeyError as e:
            logger.error(
                f"problem constructing {self}: data_server_url configuration key is required")
            raise e

        self.process_integral_keys(conf)

        for key in required_keys:
            try:
                value = conf.pop(key)
                if value is None:
                    if os.environ.get('DISPATCHER_DEBUG_MODE', 'yes') != 'yes':
                        logger.error(
                            f"required configuration key {key} is None")
                        raise ValueError(
                            f"None value of the required configuration key {key} is only allowed in debug mode")
                    logger.warning(
                        f"required configuration key {key} is None")
                self.__setattr__(key, value)
            except KeyError as e:
                logger.error(
                    f"problem constructing {self}: {key} configuration key is required")
                raise e

        for key in obsolete_keys:
            if conf.pop(key, None) is not None:
                logger.warning(
                    f"{key} is disregarded, since it is naturally included in the url")

        #optional config keys
        for key in conf:
            if key not in allowed_optional_keys:
                logger.error(
                             f"config key {key} is not allowed in this context")
                raise ValueError(
                    f"config key {key} is not allowed in this context")
            self.__setattr__(key, conf[key])

        #print(' --> DataServerConf')
        # for v in  vars(self):
        #    print ('attr:',v,getattr(self,v))



    @classmethod
    def from_conf_dict(cls, conf_dict, required_keys=None, allowed_keys=None):
        return DataServerConf(required_keys, allowed_keys, **conf_dict)

    @classmethod
    # NOTE this method is not used elsewhere
    # FIXME Bug? Need to use nested dict, cfg_dict['instrument'][instrument_name]
    def from_conf_file(cls, conf_file):
        logger.info(
            "\033[32mconstructing config from file %s\033[0m", conf_file)

        with open(conf_file, 'r') as ymlfile:
            cfg_dict = yaml.load(ymlfile, Loader=yaml.SafeLoader)

        return DataServerConf.from_conf_dict(cfg_dict)

    def process_integral_keys(self, conf):
    # special cases (INTEGRAL specific)
    # NOTE: these are required keys for integral,
    # so need to be done in constructor before for-loop for the other required ones
        if conf.get('data_server_remote_cache', None) is not None:
            # path to dataserver cache
            self.data_server_remote_path = conf.pop('data_server_remote_cache')
        else:
            self.data_server_remote_path = None

        if conf.get('dispatcher_mnt_point', None) is not None:
            self.dispatcher_mnt_point = os.path.abspath(conf.pop('dispatcher_mnt_point'))
            FilePath(file_dir=self.dispatcher_mnt_point).mkdir()
        else:
            self.dispatcher_mnt_point = None

        if self.dispatcher_mnt_point is not None and self.data_server_remote_path is not None:
            self.data_server_cache = os.path.join(
                self.dispatcher_mnt_point, self.data_server_remote_path)
        else:
            self.data_server_cache = None

class ConfigEnv(object):

    @property
    def origin(self):
        return getattr(self, '_origin', 'origin-unset')

    @origin.setter
    def origin(self, value):
        self._origin = value

    def __init__(self, cfg_dict, origin=None):
        self.origin = origin
        self.cfg_dict = cfg_dict

        self.logger = logger.getChild(repr(self))

        self.logger.debug(
            'initializing with origin=%s cfg_dict=%s', origin, cfg_dict)

        #print ('--> ConfigEnv')
        self._data_server_conf_dict = {}
        #print ('keys found in cfg_dict',cfg_dict.keys())

        if 'data_server' in cfg_dict.keys():
            self.logger.debug('data_server in cfg_dict')

            for instr_name in cfg_dict['data_server']:
                self.logger.debug(
                    'data_server for instrument %s in cfg_dict', instr_name)
                self.add_data_server_conf_dict(instr_name, cfg_dict)

                #print('--> data server key conf',instr_name,self._data_server_conf_dict[instr_name])

        if 'dispatcher' in cfg_dict.keys():
            self.logger.debug('dispatcher in cfg_dict')

            disp_dict = cfg_dict['dispatcher']
            products_url = disp_dict.get('products_url', None)

            self.set_conf_dispatcher(disp_dict['dispatcher_url'],
                                     disp_dict['dispatcher_port'],
                                     disp_dict['sentry_url'],
                                     disp_dict['logstash_host'],
                                     disp_dict['logstash_port'],
                                     products_url,
                                     disp_dict['dispatcher_service_url'],
                                     disp_dict['secret_key'],
                                     )

        # not used?
        if 'microservice' in cfg_dict.keys():
            mirco_dict = cfg_dict['microservice']
            self.set_conf_microservice(
                mirco_dict['microservice_url'], mirco_dict['microservice_port'])

        #
        #print('--> dispatcher key conf',disp_dict['products_url'])

        #print('--> _data_server_conf_dict', self._data_server_conf_dict)

    def get_data_server_conf_dict(self, instr_name):
        ds_dict = None
        if instr_name in self._data_server_conf_dict.keys():
            ds_dict = self._data_server_conf_dict[instr_name]

        #print ('ds_dict from get_data_server_conf_dict', ds_dict)
        return ds_dict

    def set_conf_microservice(self, url, port):
        self.microservice_url = url
        self.microservice_port = port

    def add_data_server_conf_dict(self, instr_name, _dict):
        self._data_server_conf_dict[instr_name] = _dict
        #self._data_server_conf_dict[instr_name] = DataServerConf.from_conf_dict(data_server_conf_dict)

    def set_conf_dispatcher(self, dispatcher_url, dispatcher_port, sentry_url, logstash_host, logstash_port, products_url, dispatcher_service_url, secret_key):
        # Generic to dispatcher
        #print(dispatcher_url, dispatcher_port)
        self.dispatcher_url = dispatcher_url
        self.dispatcher_port = dispatcher_port
        self.sentry_url = sentry_url
        self.logstash_host = logstash_host
        self.logstash_port = logstash_port
        self.products_url = products_url
        self.dispatcher_service_url = dispatcher_service_url
        self.secret_key = secret_key

    def get_data_serve_conf(self, instr_name):
        if instr_name in self.data_server_conf_dict.keys():
            c = self._data_server_conf_dict[instr_name]
        else:
            c = None

        return c

    @classmethod
    def from_conf_file(cls, conf_file_path, set_by=None):

        if conf_file_path is None:
            conf_file_path = conf_dir + '/conf_env.yml'
            logger.info(
                "using conf file from default dir \"%s\" (set by cdci_data_analysis module file location): %s", conf_dir, conf_file_path)

        logger.info("loading config from file: %s", conf_file_path)

        #print('conf_file_path', conf_file_path)
        with open(conf_file_path, 'r') as ymlfile:
            #print('conf_file_path', ymlfile )
            cfg_dict = yaml.load(ymlfile, Loader=yaml.SafeLoader)

        logger.debug('cfg_dict: %s', cfg_dict)
        #print ('CICCIO')
        return ConfigEnv(cfg_dict,
                         origin={'filepath': conf_file_path, 'set_by': set_by})

    def __repr__(self):
        return f"[ {self.__class__.__name__}: {getattr(self, 'origin')}: {getattr(self, 'cfg_dict', None)} ]"


    def as_dict(self):
        return {k: getattr(self, k) 
                for k in ["origin", "cfg_dict"]}