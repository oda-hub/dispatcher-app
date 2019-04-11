cdci_data_analysis
==========================================

A flexible python framework to bridge front-end and data-server
for scientific online analysis of astrophysical data



configuration
==============

#open stack machine
http://10.194.169.75

#   Sentry
invitation
- `http://10.194.169.75:9000/accept/2/afddf62f254a471bb3d858d200a5c6dd19a96317f4444e27bad2fbea176da748/`

monitor
- `http://10.194.169.75:9000/sentry/python/`

#logstash
`http://openstack-compute01:5601/`

## tunnels

to logger
-  mac `ssh -t -L 5001:localhost:5001 nx ssh -t -L 5001:localhost:5001 tramacer@openstack-compute01`

-  nx: `ssh -t -L 5001:localhost:5001 tramacer@openstack-compute01`
to monitor

- mac `ssh -t -L 5601:localhost:5601 nx ssh -t -L 5601:localhost:5601 tramacer@openstack-compute01`

-  nx `ssh -t -L 5601:localhost:5601 tramacer@openstack-compute01`

## url from laptop
`http://localhost:5601`




#Setup On Mac

- mount isdc using fuse

  PATH is the local_cache directory from conf_env.yml
  
 - `PATH=/Users/orion/astro/Integral_Web_Analysis/TEST_DISPATCHER/ddosa_cache`

 - `mkdir PATH`

 - `sshfs tramacer@nx:/unsaved_data/neronov/data PATH`

 - tunnel to intgccn01 machine
  `ssh -t -L 32778:localhost:32778 nx ssh -t -L 32778:localhost:32778 tramacer@intggcn01.isdc.unige.ch`

#Setup cidci machine
-  `cd  /var/cdci/dispatcher`
- `source conf/set_environment_dev_disp.sh` 
- cd to work dir `/var/cdci/dispatcher/online_analysis/XXX/workdir`
- single thread: `run_osa_cdci_server.py -conf_file conf_env.yml`
- multi thread: `run_osa_cdci_server.py -conf_file conf_env.yml -use_gunicorn `

#on every machine
cp .secret-ddosa-client ~


#XSPEC python

- login to cdciweb01

- run always on cdciweb01 machine!

- bash
- heainint

## Xspec python install

- include: python-config --cflags
- lib:     python-config --ldflags

in heasoft-<ver>/Xspec/BUILD_DIR/hmakerc

`PYTHON_INC="-I/home/isdc/tramacer/anaconda2/include/python2.7"`

`PYTHON_LIB="-lpython2.7"`


- `cd /path/to/heasoft-<ver>/Xspec/src/XSUser/Python/xspec`
- `hmake clean`
- `hmake`
- `hmake install`


#To run tests example in run_test directory:

- ```pytest ../cdci_data_analysis/tests/test_plugins.py::test_asynch_full -s -v```


What's the license?
-------------------

cdci_data_analysis is distributed under the terms of The MIT License.

Who's responsible?
-------------------
Andrea Tramacere

ISDC Data Centre for Astrophysics, Astronomy Department of the University of Geneva, Chemin d'Ecogia 16, CH-1290 Versoix, Switzerland
