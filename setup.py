
from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = 'andrea tramacere'


#!/usr/bin/env python

from setuptools import setup, find_packages
import glob
import json

# f = open("./requirements.txt",'r')
# install_req=f.readlines()
# f.close()
# install_req = [x for x in install_req if not x.startswith('-e git+h')]

install_req = [
    'oda_api>=1.1.31',
    'pylogstash_context>=0.1.19',
    "numpy",
    "pyyaml",
    "simplejson",
    "flask",
    "astropy>=2.0.3",
    "gunicorn",
    "decorator",
    "python-logstash",
    "raven",
    "blinker",
    "bokeh==2.4.2",
    "json_tricks",
    "flask-restx",
    "six",
    "werkzeug==2.0.3",
    "python-shell-colors",
    "logging_tree",
    "celery",
    "redis",
    "pyjwt",
    "marshmallow",
    "black",
    "bs4",
    "GitPython",
    "nbformat",
    "giturlparse"
]

test_req = [
    'psutil',
]

packs = find_packages()

print ('packs',packs)

with open('cdci_data_analysis/pkg_info.json') as fp:
    _info = json.load(fp)

__version__ = _info['version']

include_package_data=True

scripts_list=glob.glob('./bin/*')
setup(name='cdci_data_analysis',
      version=__version__,
      description='A Python Framework for CDCI online data analysis',
      author='Andrea Tramacere',
      author_email='andrea.tramacere@unige.ch',
      scripts=scripts_list,
      package_data={'cdci_data_analysis':['config_dir/*']},
      packages=packs,
      include_package_data=True,
      install_requires=install_req,
      extras_require={
          'test': test_req
      }
      )
