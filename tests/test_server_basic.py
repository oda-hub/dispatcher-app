import subprocess
import requests
import time
import re
import json
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


default_params = dict(
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


# why ~1 second? so long
def ask(server, params, expected_query_status, expected_job_status=None, max_time_s=2.0, expected_status_code=200):
    t0 = time.time()
    c=requests.get(server + "/run_analysis",
                   params={**params},
                  )
    print(f"\033[31m request took {time.time() - t0} seconds\033[0m")
    t_spent = time.time() - t0
    assert t_spent < max_time_s

    print("content:", c.text[:1000])
    if len(c.text) > 1000:
        print(".... (truncated)")

    jdata=c.json()

    if expected_status_code is not None:
        assert c.status_code == expected_status_code

    print(list(jdata.keys()))

    if expected_job_status is not None:
        assert jdata["exit_status"]["job_status"] in expected_job_status

    if expected_query_status is not None:
        assert jdata["query_status"] in expected_query_status

    return jdata


def loop_ask(params):
    jdata = ask(server,
                {**params, 
                 'async_dispatcher': True,
                 'query_status': 'new',
                },
                expected_query_status=["submitted"])

    last_status = jdata["query_status"]

    t0 = time.time()

    tries_till_reset = 20

    while True:
        if tries_till_reset <= 0:
            next_query_status = "ready"
            print("\033[1;31;46mresetting query status to new, too long!\033[0m")
            tries_till_reset = 20
        else:
            next_query_status = jdata['query_status']
            tries_till_reset -= 1

        jdata = ask(server,
                    {**params, "async_dispatcher": True,
                               'query_status': next_query_status,
                               'job_id': jdata['job_monitor']['job_id'],
                               'session_id': jdata['session_id']},
                    expected_query_status=["submitted", "done"],
                    max_time_s=3,
                    )

        if jdata["query_status"] in ["ready", "done"]:
            print("query READY:", jdata["query_status"])
            break

        print("query NOT-READY:", jdata["query_status"], jdata["job_monitor"])
        print("looping...")

        time.sleep(5)


    print(f"\033[31m total request took {time.time() - t0} seconds\033[0m")

    return jdata, time.time() - t0

def validate_no_data_products(jdata):
    assert jdata["exit_status"]["debug_message"] == "{\"node\": \"dataanalysis.core.AnalysisException\", \"exception\": \"{}\", \"exception_kind\": \"handled\"}"
    assert jdata["exit_status"]["error_message"] == "AnalysisException:{}"
    assert jdata["exit_status"]["message"] == "failed: get dataserver products "
    assert jdata["job_status"] == "failed"

@pytest.mark.parametrize("selection", ["range", "280200470010.001"])
def test_isgri_image_no_pointings(dispatcher_live_fixture, selection):
    """
    this will reproduce the entire flow of frontend-dispatcher, apart from receiving callback
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

    if selection == "range":
        params = {
            **default_params,
            'T1': "2008-01-01T11:11:11.0",
            'T2': "2009-01-01T11:11:11.0",
            'max_pointings': 1,
            'async_dispatcher': False,
        }
    else:
        params = {
            **default_params,
            'scw_list': selection,
            'async_dispatcher': False,
        }

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=50,
                )
    
    print(list(jdata.keys()))

    validate_no_data_products(jdata)



def test_isgri_image_fixed_done(dispatcher_live_fixture):
    """
    something already done at backend
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

    jdata = ask(server,
                {**default_params, "async_dispatcher": False},
                expected_query_status=["done"],
                max_time_s=50,
                )

    print(jdata)

    json.dump(jdata, open("jdata.json", "w"))


def test_isgri_image_fixed_done_async_postproc(dispatcher_live_fixture):
    """
    something already done at backend
    new session every time, hence re-do post-process
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

    params = {
       **default_params,
    }

    jdata, tspent = loop_ask(params)

    assert time.time() - t0_total > 20
    assert time.time() - t0_total < 40



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
                   
    
    params = {
       **default_params,
       'E2_keV':emax,
    }

    jdata, tspent = loop_ask(params)



