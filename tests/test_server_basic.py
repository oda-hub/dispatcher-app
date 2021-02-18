import subprocess
import requests
import time
import re
import signal
import os
import traceback

from threading import Thread
from time import sleep

import pytest

#pytestmark = pytest.mark.skip("these tests still WIP")


def test_no_instrument(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    c=requests.get(server + "/run_analysis",
                   params=dict(
                   image_type="Real",
                   product_type="image",
                   E1=20.,
                   E2=40.,
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
                       E1=20.,
                       E2=40.,
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

def test_isgri_image_instrument(dispatcher_live_fixture):
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
                       E1=20.,
                       E2=40.,
                       T1="2008-01-01T11:11:11.0",
                       T2="2009-01-01T11:11:11.0",
                       max_pointings=1,
                    )
                  )

    print("content:", c.text)

    jdata=c.json()
    
    assert c.status_code == 200

    print(list(jdata.keys()))

    assert jdata["exit_status"]["job_status"] == "submitted"


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
                       E1=20.,
                       E2=40.,
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


