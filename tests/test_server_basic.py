import subprocess
import requests
import time
import re
import signal
import os
import random
import traceback

from threading import Thread
from time import sleep

import pytest

#pytestmark = pytest.mark.skip("these tests still WIP")


"""
this will reproduce the entire flow of frontend-dispatcher, apart from receiving callback
"""


def test_no_instrument(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    c=requests.get(server + "/run_analysis",
                   params=dict(
                   image_type="Real",
                   product_type="image",
                   E1_keV=20.,
                   E2_keV=40.,
                   T1="2008-01-01T11:11:11.0",
                   T2="2008-06-01T11:11:11.0",
                ))

    print("content:", c.text)

    jdata=c.json()

    assert c.status_code == 400

def test_isgri_no_osa(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    c=requests.get(server + "/run_analysis",
                   params=dict(
                       query_status="new",
                       query_type="Real",
                       instrument="isgri",
                       product_type="isgri_image",
                       E1_keV=20.,
                       E2_keV=40.,
                       T1="2008-01-01T11:11:11.0",
                       T2="2008-06-01T11:11:11.0",
                    )
                  )

    print("content:", c.text)

    jdata=c.json()
    print('done')
    print(list(jdata.keys()))

    assert c.status_code == 400

    assert jdata["error_message"] == "osa_version is needed"


def test_isgri_image_no_pointings(dispatcher_live_fixture):
    """
    this will reproduce the entire flow of frontend-dispatcher, apart from receiving callback
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

    c=requests.get(server + "/run_analysis",
                   params=dict(
                       query_status="new",
                       query_type="Real",
                       instrument="isgri",
                       product_type="isgri_image",
                       osa_version="OSA10.2",
                       E1_keV=20.,
                       E2_keV=40.,
                       T1="2008-01-01T11:11:11.0",
                       T2="2009-01-01T11:11:11.0",
                       max_pointings=1,
                    )
                  )

    print("content:", c.text)

    jdata=c.json()
    
    assert c.status_code == 200

    print(list(jdata.keys()))

    assert jdata["exit_status"]["debug_message"] == "{\"node\": \"dataanalysis.core.AnalysisException\", \"exception\": \"{}\", \"exception_kind\": \"handled\"}"
    assert jdata["exit_status"]["error_message"] == "AnalysisException:{}"
    assert jdata["exit_status"]["message"] == "failed: get dataserver products "
    assert jdata["job_status"] == "failed"


def test_isgri_image_fixed_done(dispatcher_live_fixture):
    """
    something already done at backend
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

    t0 = time.time()
    c=requests.get(server + "/run_analysis",
                   params=dict(
                       query_status="new",
                       query_type="Real",
                       instrument="isgri",
                       product_type="isgri_image",
                       osa_version="OSA10.2",
                       E1_keV=20.,
                       E2_keV=40.,
                       T1="2008-01-01T11:11:11.0",
                       T2="2009-01-01T11:11:11.0",
                       max_pointings=2,
                       RA=83,
                       DEC=22,
                       radius=6,
                       async_dispatcher=False,
                    )
                  )

    print(f"\033[31m request took {time.time() - t0} seconds\033[0m")

    assert time.time() - t0 > 15

    print("content:", c.text[:1000])
    if len(c.text) > 1000:
        print(".... (truncated)")

    jdata=c.json()
    
    assert c.status_code == 200

    print(list(jdata.keys()))

    assert jdata["exit_status"]["job_status"] == "done"

def test_isgri_image_fixed_done_async_postproc(dispatcher_live_fixture):
    """
    something already done at backend
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

    params=dict(
       query_status="new",
       query_type="Real",
       instrument="isgri",
       product_type="isgri_image",
       osa_version="OSA10.2",
       E1_keV=20.,
       E2_keV=40.,
       T1="2008-01-01T11:11:11.0",
       T2="2009-01-01T11:11:11.0",
       max_pointings=2,
       RA=83,
       DEC=22,
       radius=6,
    )

    t0 = time.time()
    c=requests.get(server + "/run_analysis",
                   params={**params, 'async_dispatcher': True},
                  )
    print(f"\033[31m request took {time.time() - t0} seconds\033[0m")
    assert time.time() - t0 < 2

    print("content:", c.text[:1000])
    if len(c.text) > 1000:
        print(".... (truncated)")

    jdata=c.json()
    assert c.status_code == 200

    print(list(jdata.keys()))

    assert jdata["exit_status"]["job_status"] == "post-processing"

    ########

    while True:
        t0 = time.time()
        c=requests.get(server + "/run_analysis",
                       params={**params, 
                               'query_status': jdata['query_status'],
                               'job_id': jdata['job_monitor']['job_id'],
                               'session_id': jdata['session_id'],
                               'async_dispatcher': True,
                              }
                      )
        print(f"\033[31m request took {time.time() - t0} seconds\033[0m")
        assert time.time() - t0 < 10 # this might be longer due to serialization, store otherwise!

        print("content:", c.text[:1000])
        if len(c.text) > 1000:
            print(".... (truncated)")

        jdata=c.json()
        assert c.status_code == 200

        print(list(jdata.keys()))

        print(f"\033[31mquery status \"{jdata['query_status']}\" job status {jdata['exit_status']['job_status']} \033[0m")
        
        if jdata["query_status"] == "done":
            print("query READY:", jdata["query_status"])
            break

        time.sleep(1)


def test_isgri_image_random_emax(dispatcher_live_fixture):
    """
    something already done at backend
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

    try:
        emax = int(open("emax-last", "rt").read())
    except:
        emax = random.randint(30, 800) # but sometimes it's going to be done
        open("emax-last", "wt").write("%d"%emax)
                   
    params=dict(
       query_status="new",
       query_type="Real",
       instrument="isgri",
       product_type="isgri_image",
       osa_version="OSA10.2",
       E1_keV=20.,
       E2_keV=emax,
       T1="2008-01-01T11:11:11.0",
       T2="2009-01-01T11:11:11.0",
       max_pointings=2,
       RA=83,
       DEC=22,
       radius=6,
    )

    t0 = time.time()
    c = requests.get(server + "/run_analysis",
                   params=params,
                )
    print(f"\033[31m request took {time.time() - t0} seconds\033[0m")

    print("content:", c.text[:1000])
    if len(c.text) > 1000:
        print(".... (truncated)")

    jdata=c.json()
    
    assert c.status_code == 200

    print(list(jdata.keys()))

    assert jdata["exit_status"]["job_status"] == "submitted"
    assert jdata["query_status"] == "submitted"

    last_status = jdata["query_status"]

    n = 10
    while True:
        t0 = time.time()
        c = requests.get(
                    server + "/run_analysis",
                    params={**params, 
                                'query_status': jdata['query_status'],
                                'job_id': jdata['job_monitor']['job_id'],
                                'session_id': jdata['session_id'],
                           },
                  )
        print(f"\033[31m request took {time.time() - t0} seconds\033[0m")

        if jdata["query_status"] in ["ready", "done"]:
            print("query READY:", jdata["query_status"])
            break

        print("query NOT-READY:", jdata["query_status"], jdata["job_monitor"])
        print("looping...")

        time.sleep(5)
        n -= 1

        if n <= 0: break # since callback will not be treated



