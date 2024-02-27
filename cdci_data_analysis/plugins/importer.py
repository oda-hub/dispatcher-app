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

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f
import importlib
import pkgutil
import traceback
import os
import logging
from pscolors import render
logger = logging.getLogger(__name__)
import sys
from importlib import reload
from cdci_data_analysis.analysis.instrument import InstrumentFactoryIterator


#plugin_list=['cdci_osa_plugin','cdci_polar_plugin']

cdci_plugins_dict = {
    name: importlib.import_module(name)
    for finder, name, ispkg
    in pkgutil.iter_modules()
    if (name.startswith('cdci') and name.endswith('plugin')) or \
       (name.startswith('dispatcher_plugin_'))
}

if os.environ.get('DISPATCHER_DEBUG_MODE', 'no') == 'yes':
    cdci_plugins_dict['dummy_plugin'] = importlib.import_module('.dummy_plugin', 'cdci_data_analysis.plugins')

def build_instrument_factory_iter():
    activate_plugins = os.environ.get('DISPATCHER_PLUGINS', 'auto')
    instr_factory_iter = InstrumentFactoryIterator()
    
    for plugin_name in cdci_plugins_dict:
        if activate_plugins == 'auto' or plugin_name in activate_plugins:   
            logger.info("found plugin: %s", plugin_name)

            try:
                e = importlib.import_module('.exposer', cdci_plugins_dict[plugin_name].__name__)
                instr_factory_iter.extend(e.instr_factory_list)
                logger.info(render('{GREEN}imported plugin: %s{/}'), plugin_name)

            except Exception as e:
                logger.error('failed to import %s: %s', plugin_name,e )
                traceback.print_exc()
    return instr_factory_iter

instrument_factory_iter = build_instrument_factory_iter()

def reload_plugin(plugin_name):
    global instrument_factory_iter
    if plugin_name not in cdci_plugins_dict.keys():
        raise ModuleNotFoundError(plugin_name)
    reload(cdci_plugins_dict[plugin_name])
    reload(sys.modules[cdci_plugins_dict[plugin_name].__name__+'.exposer'])
    instrument_factory_iter = build_instrument_factory_iter()