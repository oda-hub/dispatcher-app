#!/usr/bin/env python


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



import argparse
import time
import yaml

def run(job_id,session_id,scratch_dir,N):
    job_status = {}

    job_status['session_id'] = session_id
    job_status['job_id']= job_id
    job_status['status'] = 'submitted'
    job_status['fraction'] = 0.0
    f_path = scratch_dir + '/' + 'status.yml'
    for i in range(N+1):
        #print('i', i)
        time.sleep(1)
        job_status['fraction'] = float(i) / (N)
        with open(f_path, 'w') as outfile:
            yaml.dump(job_status, outfile, default_flow_style=True, encoding=('utf-8'))
    #
    job_status['status'] = 'done'

    print ('writing job status',job_status)
    with open(f_path, 'w') as outfile:
        yaml.dump(job_status, outfile, default_flow_style=True,encoding=('utf-8'))


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('job_id', type=str)
    parser.add_argument('session_id', type=str)
    parser.add_argument('scratch_dir', type=str)
    parser.add_argument('N', type=int)

    args = parser.parse_args()

    job_id=args.job_id
    session_id = args.session_id
    scratch_dir = args.scratch_dir
    N=args.N

    run(job_id,session_id,scratch_dir,N)

if __name__ == "__main__":
    main()
