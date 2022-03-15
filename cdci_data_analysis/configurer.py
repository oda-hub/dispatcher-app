from __future__ import absolute_import, division, print_function
import traceback

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

from typing import Union, List

import sys
import os
import logging

from typing import List, Union
from cdci_data_analysis import conf_dir
from cdci_data_analysis.analysis.io_helper import FilePath
import yaml

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

    @property
    def legacy_plugin12_allowed(self) -> bool:
        return os.environ.get('DISPATCHER_LEGACY_PLUGIN12_ALLOWED', 'yes') == 'yes'

    def __getattr__(self, k):
        if self.legacy_plugin12_allowed:
            if k in self.obsolete_keys:
                logger.warning("attempting to access obsolete key %s, returning None", k)
                return None
        
        raise AttributeError

    def __init__(self, 
                 required_keys: Union[List[str], None]=None, 
                 allowed_keys: Union[List[str], None]=None, 
                 **kwargs):

        if required_keys is None:
            #temporary hardcode to preserve interface
            required_keys = ['data_server_url', 'dummy_cache']
            # also integral specific, but treated seperately
        else:
            required_keys = required_keys.copy()

        if allowed_keys is None:
            #temporary hardcode smth to preserve interface
            allowed_optional_keys = ['data_server_cache']

            # preserving interface requires listing these keys, currently provided by all plugins (to be updated)
            allowed_optional_keys += ['data_server_remote_cache', 'dispatcher_mnt_point']
        else:
            allowed_optional_keys = [x for x in allowed_keys if x not in required_keys]

        self.obsolete_keys = ('data_server_port', 'data_server_host')

        conf = kwargs.copy()

        logger.info("building config from %s", conf)
        logger.info("allowed keys %s", allowed_keys)

        context_details_message = \
        f"""
        required_keys: {required_keys}
        allowed_keys: {allowed_keys}
        allowed_optional_keys: {allowed_optional_keys}
        obsolete_keys: {self.obsolete_keys}

        conf: {conf}
        """

        try:
            self.data_server_url = conf.pop('data_server_url')
            required_keys.remove('data_server_url')
        except KeyError as e:
            logger.error(
                f"problem constructing {self}: data_server_url configuration key is required")
            raise e

        self.process_integral_keys(conf, required_keys)

        for key in required_keys:
            try:
                value = conf.pop(key)
                if value is None:
                    if os.environ.get('DISPATCHER_DEBUG_MODE', 'no') != 'yes':
                        logger.error(
                            f"required configuration key {key} is None")
                        raise ValueError(
                            f"None value of the required configuration key {key} is only allowed in debug mode")
                    logger.warning(
                        f"required configuration key {key} is None\n" + context_details_message)
                self.__setattr__(key, value)
            except KeyError as e:
                logger.error(
                    f"problem constructing {self}: {key} configuration key is required\n" + context_details_message)
                raise e

        for key in self.obsolete_keys:
            if conf.pop(key, None) is not None:
                logger.warning(
                    f"{key} is disregarded, since it is naturally included in the url")

        #optional config keys
        for key in conf:
            if key not in allowed_optional_keys:
                m = f"config key {key} is not allowed in this context!'\n" + context_details_message                
                logger.error(m)
                raise KeyError(m)
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

    def process_integral_keys(self, conf, required_keys):
    # special cases (INTEGRAL specific)
    # NOTE: these could appear in required_keys for Integral (if explicitly specified),
    # so need to be done in constructor before for-loop for the other required ones
        if conf.get('data_server_remote_cache', None) is not None:
            # path to dataserver cache
            self.data_server_remote_path = conf.pop('data_server_remote_cache')
        else:
            if 'data_server_remote_cache' in required_keys:
                raise KeyError("data_server_remote_cache configuration key is required")

        if conf.get('dispatcher_mnt_point', None) is not None:
            self.dispatcher_mnt_point = os.path.abspath(conf.pop('dispatcher_mnt_point'))
            FilePath(file_dir=self.dispatcher_mnt_point).mkdir()
        else:
            if 'dispatcher_mnt_point' in required_keys:
                raise KeyError("dispatcher_mnt_point configuration key is required")

        try:
            required_keys.remove('data_server_remote_cache')
            required_keys.remove('dispather_mnt_point')
        except ValueError:
            pass

        if hasattr(self, 'dispatcher_mnt_point') and hasattr(self, 'data_server_remote_path'):
            self.data_server_cache = os.path.join(
                self.dispatcher_mnt_point, self.data_server_remote_path)

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

            self.set_conf_dispatcher(disp_dict['bind_options']['bind_host'],
                                     disp_dict['bind_options']['bind_port'],
                                     disp_dict['sentry_url'],
                                     disp_dict['logstash_host'],
                                     disp_dict['logstash_port'],
                                     products_url,
                                     disp_dict['dispatcher_callback_url_base'],
                                     disp_dict['secret_key'],
                                     disp_dict['email_options']['smtp_server'],
                                     disp_dict['email_options']['sender_email_address'],
                                     disp_dict['email_options']['cc_receivers_email_addresses'],
                                     disp_dict['email_options']['bcc_receivers_email_addresses'],
                                     disp_dict['email_options']['smtp_port'],
                                     disp_dict['email_options']['smtp_server_password'],
                                     disp_dict['email_options']['email_sending_timeout'],
                                     disp_dict['email_options']['email_sending_timeout_default_threshold'],
                                     disp_dict['email_options']['email_sending_job_submitted'],
                                     disp_dict['email_options']['email_sending_job_submitted_default_interval'],
                                     disp_dict.get('product_gallery_options', {}).get('product_gallery_url', None),
                                     disp_dict.get('product_gallery_options', {}).get('product_gallery_secret_key', None),
                                     disp_dict.get('product_gallery_options', {}).get('name_resolver_url', None),
                                     disp_dict.get('product_gallery_options', {}).get('entities_portal_url', None),
                                     disp_dict.get('renku_options', {}).get('renku_gitlab_repository_url', None),
                                     disp_dict.get('renku_options', {}).get('renku_gitlab_user_name', None),
                                     disp_dict.get('renku_options', {}).get('renku_project_url', None),
                                     disp_dict.get('renku_options', {}).get('ssh_key_file', None)
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

    def set_conf_dispatcher(self,
                            bind_host,
                            bind_port,
                            sentry_url,
                            logstash_host,
                            logstash_port,
                            products_url,
                            dispatcher_callback_url_base,
                            secret_key,
                            smtp_server,
                            sender_email_address,
                            cc_receivers_email_addresses,
                            bcc_receivers_email_addresses,
                            smtp_port,
                            smtp_server_password,
                            email_sending_timeout,
                            email_sending_timeout_default_threshold,
                            email_sending_job_submitted,
                            email_sending_job_submitted_default_interval,
                            product_gallery_url,
                            product_gallery_secret_key,
                            name_resolver_url,
                            entities_portal_url,
                            renku_gitlab_repository_url,
                            renku_gitlab_user_name,
                            renku_project_url,
                            renku_gitlab_ssh_key_file
                            ):
        # Generic to dispatcher
        #print(dispatcher_url, dispatcher_port)
        self.bind_host = bind_host
        self.bind_port = bind_port
        self.sentry_url = sentry_url
        self.logstash_host = logstash_host
        self.logstash_port = logstash_port
        self.products_url = products_url
        self.dispatcher_callback_url_base = dispatcher_callback_url_base
        self.secret_key = secret_key
        self.smtp_server = smtp_server
        self.sender_email_address = sender_email_address
        self.cc_receivers_email_addresses = cc_receivers_email_addresses
        self.bcc_receivers_email_addresses = bcc_receivers_email_addresses
        self.smtp_port = smtp_port
        self.smtp_server_password = smtp_server_password
        self.email_sending_timeout = email_sending_timeout
        self.email_sending_timeout_default_threshold = email_sending_timeout_default_threshold
        self.email_sending_job_submitted = email_sending_job_submitted
        self.email_sending_job_submitted_default_interval = email_sending_job_submitted_default_interval
        self.product_gallery_url = product_gallery_url
        self.product_gallery_secret_key = product_gallery_secret_key
        self.name_resolver_url = name_resolver_url
        self.entities_portal_url = entities_portal_url
        self.renku_gitlab_repository_url = renku_gitlab_repository_url
        self.renku_gitlab_ssh_key_file = renku_gitlab_ssh_key_file
        self.renku_gitlab_user_name = renku_gitlab_user_name
        self.renku_project_url = renku_project_url

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
