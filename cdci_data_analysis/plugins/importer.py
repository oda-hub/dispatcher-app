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
from .dummy_instrument import empty_instrument, empty_async_instrument, empty_semi_async_instrument
logger = logging.getLogger(__name__)
import sys
from importlib import reload

#plugin_list=['cdci_osa_plugin','cdci_polar_plugin']

cdci_plugins_dict = {
    name: importlib.import_module(name)
    for finder, name, ispkg
    in pkgutil.iter_modules()
    if (name.startswith('cdci') and name.endswith('plugin')) or \
       (name.startswith('dispatcher_plugin_'))
}

def build_instrument_factory_list():
    instr_factory_list = []
    # pre-load the empty instrument factory

    # if not in debug mode, these instruments are not needed
    if os.environ.get('DISPATCHER_DEBUG_MODE', 'no') == 'yes':
        instr_factory_list.append(empty_instrument.my_instr_factory)
        instr_factory_list.append(empty_async_instrument.my_instr_factory)
        instr_factory_list.append(empty_semi_async_instrument.my_instr_factory)

    for plugin_name in cdci_plugins_dict:
        logger.info("found plugin: %s", plugin_name)

        try:
            e = importlib.import_module(plugin_name+'.exposer')
            instr_factory_list.extend(e.instr_factory_list)
            logger.info(render('{GREEN}imported plugin: %s{/}'), plugin_name)

        except Exception as e:
            logger.error('failed to import %s: %s', plugin_name,e )
            traceback.print_exc()
    return instr_factory_list

instrument_factory_list = build_instrument_factory_list()

def reload_plugin(plugin_name):
    global instrument_factory_list
    if plugin_name not in cdci_plugins_dict.keys():
        raise ModuleNotFoundError(plugin_name)
    reload(cdci_plugins_dict[plugin_name])
    reload(sys.modules[plugin_name+'.exposer'])
    instrument_factory_list = build_instrument_factory_list()