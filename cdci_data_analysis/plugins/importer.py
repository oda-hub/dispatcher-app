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
import importlib
import pkgutil
import traceback

import logging
from pscolors import render

logger = logging.getLogger(__name__)

#plugin_list=['cdci_osa_plugin','cdci_polar_plugin']

cdci_plugins_dict = {
    name: importlib.import_module(name)
    for finder, name, ispkg
    in pkgutil.iter_modules()
    if (name.startswith('cdci') and name.endswith('plugin'))
}

instrument_factory_list=[]
for plugin_name in cdci_plugins_dict:
    logger.info("found plugin: %s", plugin_name)

    try:
        e=importlib.import_module(plugin_name+'.exposer')
        instrument_factory_list.extend(e.instr_factory_list)
        logger.info(render('{GREEN}imported plugin: %s{/}'), plugin_name)

    except Exception as e:
        logger.error('failed to import %s: %s', plugin_name,e )
        traceback.print_exc()
