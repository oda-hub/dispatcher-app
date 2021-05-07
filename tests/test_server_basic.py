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
import glob
import pytest
from functools import reduce
import yaml

#pytestmark = pytest.mark.skip("these tests still WIP")

from cdci_data_analysis.pytest_fixtures import loop_ask, ask


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


default_exp_time = int(time.time()) + 5000
default_token_payload = dict(
    sub="mtm@mtmco.net",
    name="mmeharga",
    roles="general",
    exp=default_exp_time,
    tem=0,
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
    assert jdata['installed_instruments'] == ['empty', 'empty-async', 'isgri', 'jemx', 'osa_fake', 'spi_acs'] or \
           jdata['installed_instruments'] == ['empty', 'empty-async', 'spi_acs'] or \
           jdata['installed_instruments'] == ['empty', 'empty-async'] or \
           jdata['installed_instruments'] == []

    assert jdata['debug_mode'] == "yes"
    assert 'dispatcher-config' in jdata['config']

    dispatcher_config = jdata['config']['dispatcher-config']

    assert 'origin' in dispatcher_config

    assert 'sentry_url' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_port' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_host' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'secret_key' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'smtp_server_password' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'products_url' in dispatcher_config['cfg_dict']['dispatcher']

    logger.info(jdata['config'])


def test_same_request_different_users(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
    # let's generate two valid tokens, for two different users
    token_payload_1 = {
        **default_token_payload,
        "sub":"mtm1@mtmco.net"
    }
    encoded_token_1 = jwt.encode(token_payload_1, secret_key, algorithm='HS256')
    token_payload_2 = {
        **default_token_payload,
        "sub": "mtm2@mtmco.net"
    }
    encoded_token_2 = jwt.encode(token_payload_2, secret_key, algorithm='HS256')

    # issuing a request each, with the same set of parameters
    params_1 = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token_1
    }

    jdata_1 = ask(server,
                  params_1,
                  expected_query_status=["done"],
                  max_time_s=50,
                  )

    assert jdata_1["exit_status"]["debug_message"] == ""
    assert jdata_1["exit_status"]["error_message"] == ""
    assert jdata_1["exit_status"]["message"] == ""

    session_id_1 = jdata_1['session_id']
    job_id_1 = jdata_1['job_monitor']['job_id']

    params_2 = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token_2
    }
    jdata_2 = ask(server,
                  params_2,
                  expected_query_status=["done"],
                  max_time_s=50,
                  )

    assert jdata_2["exit_status"]["debug_message"] == ""
    assert jdata_2["exit_status"]["error_message"] == ""
    assert jdata_2["exit_status"]["message"] == ""

    session_id_2 = jdata_2['session_id']
    job_id_2 = jdata_2['job_monitor']['job_id']

    assert job_id_1 != job_id_2

    dir_list_1 = glob.glob('*_jid_%s*' % job_id_1)
    dir_list_2 = glob.glob('*_jid_%s*' % job_id_2)
    assert len(dir_list_1) == len(dir_list_2)


def test_valid_token(dispatcher_live_fixture):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # let's generate a valid token
    token_payload = {
        **default_token_payload,
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
                expected_query_status=["done"],
                max_time_s=50,
                )

    assert jdata["exit_status"]["debug_message"] == ""
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == ""

    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))


@pytest.mark.not_safe_parallel
def test_invalid_token(dispatcher_live_fixture, ):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # let's generate an expired token
    exp_time = int(time.time()) - 500
    # expired token
    token_payload = {
        **default_token_payload,
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

    # count the number of scratch folders
    dir_list = glob.glob('scratch_*')
    number_scartch_dirs = len(dir_list)

    jdata = ask(server,
                params,
                max_time_s=50,
                expected_query_status=None,
                expected_status_code=403
                )

    assert jdata['error_message'] == 'token expired'
    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))

    # certain output information should not even returned
    assert 'session_id' not in jdata
    assert 'job_monitor' not in jdata

    # count again
    dir_list = glob.glob('scratch_*')
    assert number_scartch_dirs == len(dir_list)


@pytest.mark.odaapi
def test_valid_token_oda_api(dispatcher_live_fixture):
    import oda_api.api

    # let's generate a valid token
    token_payload = {
        **default_token_payload
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    disp = oda_api.api.DispatcherAPI(
        url=dispatcher_live_fixture)
    product = disp.get_product(
        query_status="new",
        product_type="Dummy",
        instrument="empty",
        product="dummy",
        osa_version="OSA10.2",
        E1_keV=40.0,
        E2_keV=200.0,
        scw_list=["066500220010.001"],
        token=encoded_token
    )

    logger.info("product: %s", product)
    logger.info("product show %s", product.show())

    session_id = disp.session_id
    job_id = disp.job_id

    # check query output are generated
    query_output_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/query_output.json'
    # the aliased version might have been created
    query_output_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/query_output.json'
    assert os.path.exists(query_output_json_fn) or os.path.exists(query_output_json_fn_aliased)
    # get the query output
    if os.path.exists(query_output_json_fn):
        f = open(query_output_json_fn)
    else:
        f = open(query_output_json_fn_aliased)

    jdata = json.load(f)

    assert jdata["status_dictionary"]["debug_message"] == ""
    assert jdata["status_dictionary"]["error_message"] == ""
    assert jdata["status_dictionary"]["message"] == ""


@pytest.mark.parametrize("roles", ["", "unige-hpc-full, general", ["unige-hpc-full", "general"]])
def test_dummy_authorization_user_roles(dispatcher_live_fixture, roles):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": roles,
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': "dummy",
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token
    }

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


@pytest.mark.parametrize("roles", ["","soldier, general", "unige-hpc-full, general"])
def test_numerical_authorization_user_roles(dispatcher_live_fixture, roles):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": roles,
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
        # let's make  a public request
        if len(roles) == 0:
            params.pop('token')
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


@pytest.mark.skip(reason="old, replaced by new tests")
@pytest.mark.parametrize("async_dispatcher", [False, True])
def test_no_token(dispatcher_live_fixture, async_dispatcher, method):
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
                method=method,
                )

    print(json.dumps(jdata, indent=4))

    assert jdata["exit_status"]["debug_message"] == ""
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == "you do not have permissions for this query, contact oda"



def flatten_nested_structure(structure, mapping, path=[]):
    if isinstance(structure, list):
        r=[flatten_nested_structure(a, mapping, path=path + [i]) for i, a in enumerate(structure)]
        return reduce(lambda x, y: x + y, r) if len(r) > 0 else r

    if isinstance(structure, dict):
        r=[flatten_nested_structure(a, mapping, path=path + [k]) for k, a in list(structure.items())]
        return reduce(lambda x,y:x+y,r) if len(r)>0 else r

    return [mapping(path, structure)]


def test_example_config(dispatcher_test_conf):
    import cdci_data_analysis.config_dir

    example_config_fn = os.path.join(
        os.path.dirname(cdci_data_analysis.__file__),
        "config_dir/conf_env.yml.example"
    )

    example_config = yaml.load(open(example_config_fn))['dispatcher']

    mapper = lambda x,y:".".join(map(str, x))
    example_config_keys = flatten_nested_structure(example_config, mapper)
    test_config_keys = flatten_nested_structure(dispatcher_test_conf, mapper)

    print("\n\n\nexample_config_keys", example_config_keys)
    print("\n\n\ntest_config_keys", test_config_keys)

    assert set(example_config_keys) == set(test_config_keys)
