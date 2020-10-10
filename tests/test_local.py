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



from flask import url_for

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

import os
import argparse
import requests

from cdci_data_analysis.flask_app.app import run_app
from cdci_data_analysis.configurer import ConfigEnv

import pytest

pytestmark = pytest.mark.skip("these tests still WIP")


def main(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('-conf_file',type=str,default=None)
    args = parser.parse_args()
    conf_file=args.conf_file
    conf= ConfigEnv.from_conf_file(conf_file)
    run_app(conf)

    return

def test_image_client(client):
    c = client.get(url_for(''), params=dict(
        product_type="image",
        E1=20.,
        E2=40.,
        T1="2008-01-01T11:11:11.0",
        T2="2008-06-01T11:11:11.0",
    ))
    jdata = c.json()
    print('b')
    print()
    list(jdata.keys())
    print()
    jdata['data']

def test_image():
    print ('a')
    c = requests.get('127.0.0.1' + "/test", params=dict(
        product_type="image",
        E1=20.,
        E2=40.,
        T1="2008-01-01T11:11:11.0",
        T2="2008-06-01T11:11:11.0",
    ))
    jdata = c.json()
    print('b')
    print()
    list(jdata.keys())
    print()
    jdata['data']



if __name__ == "__main__":
        #$port = int(os.environ.get("PORT", 5000))
    main(argv=None)
    test_image()


def test_osa_catalog():
    from cdci_data_analysis.ddosa.osa_catalog import build_osa_catalog

    catalog=build_osa_catalog('mosaic_catalog.fits')
