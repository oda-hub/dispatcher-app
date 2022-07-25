#!/usr/bin/env python


# -*- encoding: utf-8 -*-
"""

"""

from __future__ import absolute_import, division, print_function

from builtins import (bytes, open, str, super, range,
                      zip, round, input, int, pow, object, map, zip)

import os
import argparse
import multiprocessing
import gunicorn.app.base

from cdci_data_analysis.app_logging import app_logging 
from cdci_data_analysis.flask_app.app import run_app
from cdci_data_analysis.configurer import ConfigEnv




def number_of_workers():
    return (multiprocessing.cpu_count() * 2) + 1


class StandaloneApplication(gunicorn.app.base.BaseApplication):
    def __init__(self, app, options=None, app_conf=None):
        self.options = options or {}
        self.application = app
        self.app_conf = app_conf
        super(StandaloneApplication, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in self.options.items()
                       if key in self.cfg.settings and value is not None])

        for key in list(config):
            print ('conf',key.lower(), config[key])
            self.cfg.set(key.lower(), config[key])

    def load(self):
        return self.application

def main(argv=None):

    # TODO: make a conditon

    parser = argparse.ArgumentParser()
    parser.add_argument('-conf_file', type=str, default=None)
    parser.add_argument('-use_gunicorn', action='store_true')
    parser.add_argument('-debug', action='store_true', help='sets global logger debug')
    parser.add_argument('-multithread', action='store_true', help='start the dispatcher in threaded mode')
    parser.add_argument('--log-config', type=str, default=":info", help="log levels by logger, e.g. \"osa:debug,flask:info,:warning\"")

    args = parser.parse_args()

    if args.debug:
        app_logging.level_by_logger = {"": "debug"}
        app_logging.setup()
    else:
        app_logging.level_by_logger = { l.split(":")[0]:l.split(":")[1] for l in args.log_config.split(",") }
        app_logging.setup()


    black_listed_envs = ['https_proxy', 'http_proxy']

    for envvar in black_listed_envs:
        app_logging.getLogger("env").debug('removing env variable: %s',envvar)
        os.unsetenv(envvar)
        if envvar in os.environ.keys():
            del os.environ[envvar]

    conf_file = args.conf_file

    conf = ConfigEnv.from_conf_file(conf_file, 
                                    set_by=f'command line {__file__}:{__name__}')
    debug = args.debug
    multithread = args.multithread

    run_app(conf, debug=debug, threaded=multithread)


if __name__ == "__main__":
    main()
