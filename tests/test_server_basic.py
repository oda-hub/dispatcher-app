import subprocess
import requests
import time
import re
import json
import signal
import os
import random
import traceback
import logging
import jwt

from threading import Thread
from time import sleep

import pytest

#pytestmark = pytest.mark.skip("these tests still WIP")

# logger
logger = logging.getLogger(__name__)
# symmetric shared secret for the decoding of the token
secret_key = 'secretkey_test'
"""
this will reproduce the entire flow of frontend-dispatcher, apart from receiving callback
"""

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
                    token="fake-token",
                 )


def test_empty_request(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    c=requests.get(server + "/run_analysis",
                   params={},
                )

    print("content:", c.text)

    jdata=c.json()

    assert c.status_code == 400

     # parameterize this
    assert jdata['installed_instruments'] == ['empty', 'isgri', 'jemx', 'osa_fake'] or \
           jdata['installed_instruments'] == ['empty'] or \
           jdata['installed_instruments'] == []

    assert jdata['debug_mode'] == "yes"
    assert 'dispatcher-config' in jdata['config']

    dispatcher_config = jdata['config']['dispatcher-config']

    assert 'origin' in dispatcher_config

    assert 'sentry_url' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_port' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_host' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'secret_key' not in dispatcher_config['cfg_dict']['dispatcher']

    assert 'products_url' in dispatcher_config['cfg_dict']['dispatcher']

    logger.info(jdata['config'])


def test_valid_token(dispatcher_live_fixture,):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # let's generate a valid token
    exp_time = int(time.time()) + 500
    token_payload = {
        "email": "mtm@mtmco.net",
        "name": "mmeharga",
        "roles": "authenticated user ,  content manager ,  general , magic",
        "exp": exp_time
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token
    }

    jdata = ask(server,
                params,
                expected_query_status = ["done"],
                max_time_s = 50,
                )

    assert jdata["exit_status"]["debug_message"] == ""
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == ""

    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))


def test_invalid_token(dispatcher_live_fixture, ):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # let's generate an expired token
    exp_time = int(time.time()) - 500
    # expired token
    token_payload = {
        "email": "mtm@mtmco.net",
        "name": "mmeharga",
        "roles": "authenticated user, content manager, general, magic",
        "exp": exp_time
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token
    }

    jdata = ask(server,
                params,
                expected_query_status=["failed"],
                max_time_s=50,
                )

    assert jdata["exit_status"]["debug_message"] == ""
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == "token expired"

    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))


@pytest.mark.parametrize("roles", ["", "unige-hpc-full, general"])
def test_dummy_authorization_user_roles(dispatcher_live_fixture, roles):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # let's generate a valid token
    exp_time = int(time.time()) + 500
    token_payload = {
        "email": "mtm@mtmco.net",
        "name": "mmeharga",
        "roles": roles,
        "exp": exp_time
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': "dummy",
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token
    }

    # just for having the roles in a list
    roles = roles.split(',')
    roles[:] = [r.strip() for r in roles]

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=150,
                )
    assert jdata["exit_status"]["debug_message"] == ""
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == ""

    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))


@pytest.mark.parametrize("roles", ["soldier, general", "unige-hpc-full, general"])
def test_numerical_authorization_user_roles(dispatcher_live_fixture, roles):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # let's generate a valid token
    exp_time = int(time.time()) + 500
    token_payload = {
        "email": "mtm@mtmco.net",
        "name": "mmeharga",
        "roles": roles,
        "exp": exp_time
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': 'numerical',
        'query_type': "Dummy",
        'instrument': 'empty',
        'p': 55,
        'token': encoded_token
    }

    # just for having the roles in a list
    roles = roles.split(',')
    roles[:] = [r.strip() for r in roles]

    if 'unige-hpc-full' in roles:
        jdata = ask(server,
                    params,
                    expected_query_status=["done"],
                    max_time_s=150,
                    )
        assert jdata["exit_status"]["debug_message"] == ""
        assert jdata["exit_status"]["error_message"] == ""
        assert jdata["exit_status"]["message"] == ""
    else:
        jdata = ask(server,
                    params,
                    expected_query_status=["failed"],
                    max_time_s=150,
                    expected_status_code=403,
                    )
        assert jdata["exit_status"]["debug_message"] == ""
        assert jdata["exit_status"]["error_message"] == ""
        assert jdata["exit_status"]["message"] == \
               f"Roles {roles} not authorized to request the product numerical, " \
               f"[\'general\', \'unige-hpc-full\'] roles are needed"

    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))


@pytest.mark.isgri_plugin
def test_isgri_dummy(dispatcher_live_fixture):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    c = requests.get(server + "/run_analysis",
                      params = dict(
                          query_status = "new",
                          query_type = "Dummy",
                          instrument = "isgri",
                          product_type = "isgri_image",
                      ))
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(list(jdata.keys()))
    logger.info(jdata)
    assert c.status_code == 200


def test_empty_instrument_request(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
    }

    # let's keep the request public
    params.pop('token')

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=50,
                )

    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))

    assert jdata["exit_status"]["debug_message"] == ""
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == ""


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


@pytest.mark.skip(reason="todo")
@pytest.mark.isgri_plugin
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
    print(list(jdata.keys()))
    print(jdata)

    assert c.status_code == 400

    assert jdata["error_message"] == "osa_version is needed"


# why ~1 second? so long
def ask(server, params, expected_query_status, expected_job_status=None, max_time_s=2.0, expected_status_code=200):
    t0 = time.time()
    c=requests.get(server + "/run_analysis",
                   params={**params},
                  )
    logger.info(f"\033[31m request took {time.time() - t0} seconds\033[0m")
    t_spent = time.time() - t0
    assert t_spent < max_time_s

    logger.info("content: %s", c.text[:1000])
    if len(c.text) > 1000:
        print(".... (truncated)")

    jdata=c.json()

    if expected_status_code is not None:
        assert c.status_code == expected_status_code

    logger.info(list(jdata.keys()))

    if expected_job_status is not None:
        assert jdata["exit_status"]["job_status"] in expected_job_status

    if expected_query_status is not None:
        assert jdata["query_status"] in expected_query_status

    return jdata


def loop_ask(server, params):
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
            logger.info("query READY:", jdata["query_status"])
            break

        logger.info("query NOT-READY:", jdata["query_status"], jdata["job_monitor"])
        logger.info("looping...")

        time.sleep(5)

    logger.info(f"\033[31m total request took {time.time() - t0} seconds\033[0m")

    return jdata, time.time() - t0


def validate_no_data_products(jdata):
    assert jdata["exit_status"]["debug_message"] == "{\"node\": \"dataanalysis.core.AnalysisException\", \"exception\": \"{}\", \"exception_kind\": \"handled\"}"
    assert jdata["exit_status"]["error_message"] == "AnalysisException:{}"
    assert jdata["exit_status"]["message"] == "failed: get dataserver products "
    assert jdata["job_status"] == "failed"


@pytest.mark.skip(reason="old, replaced by new tests")
@pytest.mark.parametrize("async_dispatcher", [False, True])
def test_no_token(dispatcher_live_fixture, async_dispatcher):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    params = {
        **default_params,
        'async_dispatcher': async_dispatcher,
        'instrument': 'mock',
    }

    params.pop('token')

    jdata = ask(server,
                params,
                expected_query_status=["failed"],
                max_time_s=50,
                )

    print(json.dumps(jdata, indent=4))

    assert jdata["exit_status"]["debug_message"] == ""
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == "you do not have permissions for this query, contact oda"


@pytest.mark.parametrize("selection", ["range", "280200470010.001"])
@pytest.mark.dda
@pytest.mark.isgri_plugin
@pytest.mark.xfail
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
    # let's make the request public for simplicity
    params.pop('token')
    jdata = ask(server,
                params,
                expected_query_status=["failed"],
                max_time_s=50,
                )
    
    print(list(jdata.keys()))

    validate_no_data_products(jdata)


@pytest.mark.dda
@pytest.mark.isgri_plugin
def test_isgri_image_fixed_done(dispatcher_live_fixture):
    """
    something already done at backend
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

    params = {
        **default_params,
        'async_dispatcher': False,
    }
    # let's make the request public for simplicity
    params.pop('token')

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=50,
                )

    print(jdata)

    json.dump(jdata, open("jdata.json", "w"))


@pytest.mark.dda
@pytest.mark.isgri_plugin
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
    # let's make the request public for simplicity
    params.pop('token')

    jdata, tspent = loop_ask(server, params)

    assert  20 < tspent < 40


@pytest.mark.dda
@pytest.mark.isgri_plugin
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
    # let's make the request public for simplicity
    params.pop('token')
    jdata, tspent = loop_ask(server, params)



