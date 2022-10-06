import re
import shutil
import urllib

import requests
import time
import uuid
import json
import os
import logging
import jwt
import glob
import pytest
from datetime import datetime, timedelta
from dateutil import parser, tz
from functools import reduce
from urllib import parse
import nbformat as nbf
import yaml
import gzip
import random
import string

from cdci_data_analysis.analysis.catalog import BasicCatalog
from cdci_data_analysis.pytest_fixtures import DispatcherJobState, ask, make_hash, dispatcher_fetch_dummy_products
from cdci_data_analysis.flask_app.dispatcher_query import InstrumentQueryBackEnd
from cdci_data_analysis.analysis.renku_helper import clone_renku_repo, checkout_branch_renku_repo, check_job_id_branch_is_present, get_repo_path, generate_commit_request_url, generate_notebook_filename
from cdci_data_analysis.analysis.drupal_helper import execute_drupal_request, get_drupal_request_headers, get_revnum, get_observations_for_time_range, generate_gallery_jwt_token, get_user_id, get_source_astrophysical_entity_id_by_source_name
from cdci_data_analysis.plugins.dummy_plugin.data_server_dispatcher import DataServerQuery

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
                    T1="2008-01-01T11:11:11.000",
                    T2="2009-01-01T11:11:11.000",
                    T_format='isot',
                    max_pointings=2,
                    RA=83,
                    DEC=22,
                    radius=6,
                    async_dispatcher=False
                 )


default_exp_time = int(time.time()) + 5000
default_token_payload = dict(
    sub="mtm@mtmco.net",
    name="mmeharga",
    roles="general",
    exp=default_exp_time,
    tem=0,
)


@pytest.mark.fast
def test_js9(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    dispatcher_fetch_dummy_products('default')

    shutil.copy('data/dummy_prods/isgri_query_mosaic.fits', 'js9.fits')

    print("constructed server:", server)
    r = requests.get(f'{dispatcher_live_fixture.rstrip("/")}/api/v1.0/oda/get_js9_plot', params={'file_path': 'js9.fits'})
    assert r.status_code == 200

@pytest.fixture
def safe_dummy_plugin_conf():
    from cdci_data_analysis.plugins.dummy_plugin import conf_file
    with open(conf_file, 'r') as fd:
        config = fd.read()
    yield conf_file
    with open(conf_file, 'w') as fd:
        fd.write(config)

@pytest.mark.fast
def test_reload_plugin(safe_dummy_plugin_conf, dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)
    c = requests.get(server + "/api/instr-list",
                     params={'instrument': 'mock'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    assert 'empty' in jdata
    assert 'empty-async' in jdata
    assert 'empty-semi-async' in jdata
    
    with open(safe_dummy_plugin_conf, 'w') as fd:
        fd.write('instruments: []\n')
    
    c = requests.get(server + "/reload-plugin/dummy_plugin")
    assert c.status_code == 200

    c = requests.get(server + "/api/instr-list",
                     params={'instrument': 'mock'})
    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200
    # parameterize this
    assert 'empty' not in jdata
    assert 'empty-async' not in jdata
    assert 'empty-semi-async' not in jdata

@pytest.mark.fast
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
    assert sorted(jdata['installed_instruments']) == sorted(['empty', 'empty-async', 'empty-semi-async']) or \
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


def test_no_debug_mode_empty_request(dispatcher_live_fixture_no_debug_mode):
    server = dispatcher_live_fixture_no_debug_mode
    print("constructed server:", server)

    c=requests.get(server + "/run_analysis",
                   params={},
                )

    print("content:", c.text)

    jdata=c.json()

    assert c.status_code == 400

    # parameterize this
    assert jdata['installed_instruments'] == []

    assert jdata['debug_mode'] == "no"
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


@pytest.mark.fast
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

    job_id_2 = jdata_2['job_monitor']['job_id']

    assert job_id_1 != job_id_2

    dir_list_1 = glob.glob('*_jid_%s*' % job_id_1)
    dir_list_2 = glob.glob('*_jid_%s*' % job_id_2)
    assert len(dir_list_1) == len(dir_list_2)


@pytest.mark.not_safe_parallel
@pytest.mark.parametrize("request_cred", ['public', 'private'])
def test_consistency_parameters_json_dump_file(dispatcher_live_fixture, request_cred):
    DispatcherJobState.remove_scratch_folders()
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    if request_cred == 'public':
        encoded_token = None
    else:
        token_payload = {
            **default_token_payload,
            "sub": "mtm@mtmco.net"
        }

        encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    # issuing a request each, with the same set of parameters
    params = {
        **default_params,
        'query_status': "new",
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

    job_id = jdata['job_monitor']['job_id']
    session_id = jdata['session_id']
    # get the analysis_parameters json file
    analysis_parameters_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/analysis_parameters.json'
    # the aliased version might have been created
    analysis_parameters_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/analysis_parameters.json'
    assert os.path.exists(analysis_parameters_json_fn) or os.path.exists(analysis_parameters_json_fn_aliased)
    if os.path.exists(analysis_parameters_json_fn):
        analysis_parameters_json_content_original = json.load(open(analysis_parameters_json_fn))
    else:
        analysis_parameters_json_content_original = json.load(open(analysis_parameters_json_fn_aliased))

    logger.info("starting query with the same session_id and job_id")

    # issue another call, different parameters but same job_id & session_id, to simulate the Fit button
    params = {
        **default_params,
        'xspec_model': 'powerlaw',
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token,
        'session_id': session_id,
        'job_id': job_id,
        'query_status': "ready"
    }

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=50,
                )

    if os.path.exists(analysis_parameters_json_fn):
        analysis_parameters_json_content = json.load(open(analysis_parameters_json_fn))
    else:
        analysis_parameters_json_content = json.load(open(analysis_parameters_json_fn_aliased))

    assert analysis_parameters_json_content == analysis_parameters_json_content_original

@pytest.mark.fast
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

@pytest.mark.fast
@pytest.mark.parametrize("instrument", ["", "None", None, "undefined"])
def test_download_products_public(dispatcher_long_living_fixture, empty_products_files_fixture, instrument):    
    server = dispatcher_long_living_fixture

    logger.info("constructed server: %s", server)

    session_id = empty_products_files_fixture['session_id']
    job_id = empty_products_files_fixture['job_id']

    params = {
            'instrument': instrument,
            # since we are passing a job_id
            'query_status': 'ready',
            'file_list': 'test.fits.gz',
            'download_file_name': 'output_test',
            'session_id': session_id,
            'job_id': job_id
        }

    c = requests.get(server + "/download_products",
                     params=params)

    assert c.status_code == 200

    # download the output, read it and then compare it
    with open(f'scratch_sid_{session_id}_jid_{job_id}/output_test', 'wb') as fout:
        fout.write(c.content)

    with gzip.open(f'scratch_sid_{session_id}_jid_{job_id}/output_test', 'rb') as fout:
        data_downloaded = fout.read()

    assert data_downloaded == empty_products_files_fixture['content']


@pytest.mark.fast
def test_download_products_authorized_user(dispatcher_live_fixture, empty_products_user_files_fixture):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "sub": "mtm@mtmco.net",
        "mstout": True,
        "mssub": True,
        "intsub": 5
    }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    session_id = empty_products_user_files_fixture['session_id']
    job_id = empty_products_user_files_fixture['job_id']

    params = {
        # since we are passing a job_id
        'query_status': 'ready',
        'file_list': 'test.fits.gz',
        'download_file_name': 'output_test',
        'session_id': session_id,
        'job_id': job_id,
        'token': encoded_token
    }

    c = requests.get(server + "/download_products",
                     params=params)

    assert c.status_code == 200

    # download the output, read it and then compare it
    with open(f'scratch_sid_{session_id}_jid_{job_id}/output_test', 'wb') as fout:
        fout.write(c.content)

    with gzip.open(f'scratch_sid_{session_id}_jid_{job_id}/output_test', 'rb') as fout:
        data_downloaded = fout.read()

    assert data_downloaded == empty_products_user_files_fixture['content']


@pytest.mark.fast
def test_download_products_unauthorized_user(dispatcher_live_fixture, empty_products_user_files_fixture, default_params_dict):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "sub": "mtm1@mtmco.net",
        "mstout": True,
        "mssub": True,
        "intsub": 5
    }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    session_id = empty_products_user_files_fixture['session_id']
    job_id = empty_products_user_files_fixture['job_id']

    params = {
        # since we are passing a job_id
        'query_status': 'ready',
        'file_list': 'test.fits.gz',
        'download_file_name': 'output_test',
        'session_id': session_id,
        'job_id': job_id,
        'token': encoded_token
    }

    c = requests.get(server + "/download_products",
                     params=params)
    default_param_dict = default_params_dict
    default_param_dict.pop('token', None)
    default_param_dict.pop('session_id', None)
    default_param_dict.pop('job_id', None)
    wrong_job_id = make_hash(InstrumentQueryBackEnd.restricted_par_dic({**default_param_dict, "sub": "mtm1@mtmco.net"}))

    assert c.status_code == 403

    jdata = c.json()
    assert jdata["exit_status"]["debug_message"] == \
           f'The provided job_id={job_id} does not match with the ' \
           f'job_id={wrong_job_id} derived from the request parameters for your user account email; parameters are derived from recorded job state'
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == "Request not authorized"


@pytest.mark.parametrize("tem_value", [10, "10aaaa"])
@pytest.mark.parametrize("tem_key_name", ["tem", "temaaaa"])
def test_modify_token(dispatcher_live_fixture, tem_value, tem_key_name):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # expired token
    token_payload = {
        **default_token_payload,
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    token_update = {
        # new set of email options
        tem_key_name: tem_value,
        "mstout": True,
        "mssub": True,
        "msdone": True,
        "intsub": 5,
    }

    params = {
        'token': encoded_token,
        **token_update,
        'query_status': 'new',
    }

    c = requests.post(server + "/update_token_email_options",
                     params=params)

    token_payload.update(token_update)

    updated_encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    if tem_key_name == 'temaaaa':
        jdata = c.json()
        assert jdata['error_message'] == 'An error occurred while validating the following fields: ' \
                                         '{\'temaaaa\': [\'Unknown field.\']}. ' \
                                         'Please check it and re-try to issue the request'
    else:
        if tem_value == '10aaaa':
            jdata = c.json()
            assert jdata['error_message'] == 'An error occurred while validating the following fields: ' \
                                             '{\'tem\': [\'Not a valid number.\']}. ' \
                                             'Please check it and re-try to issue the request'
        else:
            payload_returned_token = jwt.decode(c.text, secret_key, algorithms='HS256')
            # order of the payload fields might change inside the dispatcher (eg by marshmallow, ordering)
            # so the two corresponding tokens might be different,
            # but the content (fields and values) are still supposed to match match
            # TODO is the order of the fields in the paylaod important?
            assert token_payload == payload_returned_token


@pytest.mark.parametrize("refresh_interval", [500000, 604800, 1000000])
def test_refresh_token(dispatcher_live_fixture, dispatcher_test_conf, refresh_interval):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # expired token
    token_payload = {
        **default_token_payload,
        "roles": "refresh-tokens"
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        'token': encoded_token,
        'query_status': 'new',
        'refresh_interval': refresh_interval
    }

    c = requests.post(server + "/refresh_token", params=params)

    if refresh_interval > dispatcher_test_conf['token_max_refresh_interval']:
        jdata = c.json()
        assert jdata['error_message'] == 'Request not authorized'
        assert jdata['debug_message'] == 'The refresh interval requested exceeds the maximum allowed, please provide a value which is lower than 604800 seconds'
    else:
        token_update = {
            "exp": default_token_payload["exp"] + refresh_interval
        }

        token_payload.update(token_update)

        payload_returned_token = jwt.decode(c.text, secret_key, algorithms='HS256')
        assert token_payload == payload_returned_token

        updated_encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
        assert c.text == updated_encoded_token


@pytest.mark.fast
@pytest.mark.not_safe_parallel
def test_invalid_token(dispatcher_live_fixture):
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

    assert jdata['error_message'] == ('The token provided is expired, please try to logout and login again. '
                                      'If already logged out, please clean the cookies, and resubmit you request.')
    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))

    # certain output information should not even returned
    assert 'session_id' not in jdata
    assert 'job_monitor' not in jdata

    # count again
    dir_list = glob.glob('scratch_*')
    assert number_scartch_dirs == len(dir_list)


@pytest.mark.fast
def test_call_back_invalid_token(dispatcher_live_fixture):
    server = dispatcher_live_fixture

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        'exp': int(time.time()) + 10,
        "tem": 0
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        token=encoded_token
    )

    DataServerQuery.set_status('submitted')
    # this should return status submitted, so email sent
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )
    assert c.status_code == 200
    jdata = c.json()

    logger.info("response from run_analysis: %s", json.dumps(jdata, indent=4))
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

    assert jdata['query_status'] == "submitted"
    assert jdata['exit_status']['job_status'] == 'submitted'
    # set the time the request was initiated
    time_request = jdata['time_request']

    # let make sure the token used for the previous request expires
    time.sleep(12)

    c = requests.get(os.path.join(server, "call_back"),
                     params=dict(
                         job_id=dispatcher_job_state.job_id,
                         session_id=dispatcher_job_state.session_id,
                         instrument_name="empty-async",
                         action='main_done',
                         node_id=f'node_0',
                         message='progressing',
                         token=encoded_token,
                         time_original_request=time_request
                     ))

    jdata = c.json()
    assert jdata['error_message'] == "The token provided is expired, please resubmit you request with a valid token."


@pytest.mark.odaapi
def test_email_oda_api(dispatcher_live_fixture, dispatcher_local_mail_server):
    DispatcherJobState.remove_scratch_folders()

    import oda_api.api

    # let's generate a valid token
    token_payload = {
        **default_token_payload
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    
    if isinstance(encoded_token, bytes):
        encoded_token = encoded_token.decode()


    disp = oda_api.api.DispatcherAPI(
        url=dispatcher_live_fixture,
        wait=False)

    for i in range(4):
        disp.get_product(
            product_type="Real",
            instrument="empty-semi-async",
            product="numerical",
            osa_version="OSA10.2",
            token=encoded_token,
            p=0,
            session_id=disp.session_id
        )
    
    dispatcher_job_state = DispatcherJobState(disp.session_id, disp.job_id)

    dispatcher_job_state.assert_email("submitted")

    disp = oda_api.api.DispatcherAPI(
        url=dispatcher_live_fixture,
        session_id=disp.session_id,
        wait=False)

    disp.get_product(
        product_type="Real",
        instrument="empty-semi-async",
        product="numerical",
        osa_version="OSA10.2",
        token=encoded_token,
        p=4
    )
    
    dispatcher_job_state = DispatcherJobState(disp.session_id, disp.job_id)
    dispatcher_job_state.assert_email("*", number=0)

    disp = oda_api.api.DispatcherAPI(
        url=dispatcher_live_fixture,
        session_id=disp.session_id,
        wait=False)

    with pytest.raises(oda_api.api.RemoteException):
        disp.get_product(
                product_type="Real",
                instrument="empty-semi-async",
                product="numerical",
                osa_version="OSA10.2",
                token=encoded_token,
                p=-1
            )

    dispatcher_job_state = DispatcherJobState(disp.session_id, disp.job_id)
    dispatcher_job_state.assert_email("*", number=0)


@pytest.mark.odaapi
def test_valid_token_oda_api(dispatcher_live_fixture):
    import oda_api.api

    # let's generate a valid token
    token_payload = {
        **default_token_payload
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    
    if isinstance(encoded_token, bytes):
        encoded_token = encoded_token.decode()

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
    assert "disp=DispatcherAPI(url='PRODUCTS_URL/dispatch-data', instrument='mock')" in jdata['prod_dictionary']['api_code'] 
    

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


@pytest.mark.parametrize("roles", ["", "soldier, general", "unige-hpc-full, general"])
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
               "Unfortunately, your priviledges are not sufficient to make the request for this particular product and parameter combination.\n"\
               f"- Your priviledge roles include {roles}\n- "\
               "You are lacking all of the following roles:\n" \
               + (" - general: general role is needed for p>50\n" if "general" not in roles else "" ) + \
               " - unige-hpc-full: unige-hpc-full role is needed for p>50 as well\n"\
               "You can request support if you think you should be able to make this request."

    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))


def test_scws_list_file(dispatcher_live_fixture):

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "unige-hpc-full, general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': 'numerical',
        'query_type': "Dummy",
        'instrument': 'empty',
        'p': 5.,
        'use_scws': 'user_file',
        'token': encoded_token
    }

    file_path = DispatcherJobState.create_p_value_file(p_value=5)

    list_file = open(file_path)

    expected_query_status = 'done'
    expected_job_status = 'done'
    expected_status_code = 200

    jdata = ask(server,
                params,
                expected_query_status=expected_query_status,
                expected_job_status=expected_job_status,
                expected_status_code=expected_status_code,
                max_time_s=150,
                method='post',
                files={'user_scw_list_file': list_file.read()}
                )

    list_file.close()
    assert 'p_list' in jdata['products']['analysis_parameters']
    assert 'use_scws' not in jdata['products']['analysis_parameters']
    assert jdata['products']['analysis_parameters']['p_list'] == ['5']
    # test job_id
    job_id = jdata['products']['job_id']
    params.pop('use_scws', None)
    # adapting some values to string
    for k, v in params.items():
        params[k] = str(v)

    restricted_par_dic = InstrumentQueryBackEnd.restricted_par_dic({
        **params,
        "p": 5.,
        "RA": 83.,
        "DEC": 22.,
        "src_name": "1E 1740.7-2942",
        "p_list": ["5"],
        "sub": "mtm@mtmco.net"}
    )
    calculated_job_id = make_hash(restricted_par_dic)

    assert job_id == calculated_job_id


@pytest.mark.test_catalog
@pytest.mark.parametrize("correct_format", [True, False])
def test_catalog_file(dispatcher_live_fixture, correct_format):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "unige-hpc-full, general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token
    }

    file_path = DispatcherJobState.create_catalog_file(catalog_value=5, wrong_format= not correct_format)

    list_file = open(file_path)

    if correct_format:
        catalog_object = BasicCatalog.from_file(file_path)
        expected_query_status = ["done"]
        expected_status_code = 200
    else:
        expected_query_status = None
        expected_status_code = 400
        error_message = ('Error while setting catalog file from the frontend : format not valid, '
                         'a catalog should be provided as a FITS (typical standard OSA catalog) or '
                         '<a href=https://docs.astropy.org/en/stable/api/astropy.io.ascii.Ecsv.html>ECSV</a> table.')

    jdata = ask(server,
                params,
                expected_query_status=expected_query_status,
                expected_status_code=expected_status_code,
                max_time_s=150,
                method='post',
                files={"user_catalog_file": list_file.read()}
                )

    if correct_format:
        list_file.close()
        assert 'selected_catalog' in jdata['products']['analysis_parameters']
        assert json.dumps(catalog_object.get_dictionary()) == jdata['products']['analysis_parameters']['selected_catalog']
        assert 'user_catalog_file' not in jdata['products']['analysis_parameters']
        # test job_id
        job_id = jdata['products']['job_id']

        # adapting some values to string
        for k, v in params.items():
            params[k] = str(v)

        restricted_par_dic = InstrumentQueryBackEnd.restricted_par_dic(
            {
                **params,
                'selected_catalog': json.dumps(catalog_object.get_dictionary()),
                'sub': 'mtm@mtmco.net',
                'p_list': [],
                'RA': 83.,
                'DEC': 22.,
                'src_name': '1E 1740.7-2942',
            }
        )
        calculated_job_id = make_hash(restricted_par_dic)

        assert job_id == calculated_job_id

    else:
        assert jdata['error_message'] == error_message


@pytest.mark.test_catalog
@pytest.mark.parametrize("correct_format", [True, False])
@pytest.mark.parametrize("catalog_selected_objects", [1, "1", "1,1,1", "", "aaa", None])
def test_user_catalog(dispatcher_live_fixture, correct_format, catalog_selected_objects):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "unige-hpc-full, general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    selected_catalog_dict = dict(
        cat_lon_name="ra",
        cat_lat_name="dec",
        cat_frame="fk5",
        cat_coord_units="deg",
        cat_column_list=[[1], ["Test A"], [6], [5], [4], [3], [2], [1], [0]],
        cat_column_names=["meta_ID", "src_names", "significance", "ra", "dec", "NEW_SOURCE", "ISGRI_FLAG", "FLAG",
                          "ERR_RAD"],
        cat_column_descr=[["meta_ID", "<i8"], ["src_names", "<U6"], ["significance", "<i8"], ["ra", "<f8"],
                          ["dec", "<f8"], ["NEW_SOURCE", "<i8"], ["ISGRI_FLAG", "<i8"], ["FLAG", "<i8"],
                          ["ERR_RAD", "<i8"]]
    )

    correct_catalog_selected_objects = catalog_selected_objects != "" and catalog_selected_objects != "aaa"

    expected_query_status = ["done"]
    expected_status_code = 200
    if not correct_format or not correct_catalog_selected_objects:
        selected_catalog_dict['cat_column_list'][8].append(0)
        expected_query_status = None
        expected_status_code = 400
        error_message = 'Error while setting catalog object : '
        if not correct_catalog_selected_objects:
            error_message += 'the selected catalog is wrongly formatted, please check your inputs'
        else:
            error_message += 'Inconsistent data column lengths: {1, 2}'

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'selected_catalog': json.dumps(selected_catalog_dict),
        'catalog_selected_objects': catalog_selected_objects,
        'token': encoded_token
    }

    jdata = ask(server,
                params,
                expected_query_status=expected_query_status,
                expected_status_code=expected_status_code,
                max_time_s=150,
                method='post'
                )
    if not correct_format or not correct_catalog_selected_objects:
        assert jdata['error_message'] == error_message
    else:
        assert 'selected_catalog' in jdata['products']['analysis_parameters']
        assert jdata['products']['analysis_parameters']['selected_catalog'] == json.dumps(selected_catalog_dict)
        assert 'user_catalog_file' not in jdata['products']['analysis_parameters']
        # test job_id
        job_id = jdata['products']['job_id']
        session_id = jdata['session_id']
        # adapting some values to string
        str_fied_params = {}
        for k, v in params.items():
            if v is not None:
                str_fied_params[k] = str(v)

        restricted_par_dic = InstrumentQueryBackEnd.restricted_par_dic(
            {
                **str_fied_params,
                'sub': 'mtm@mtmco.net',
                'p_list': [],
                'RA': 83.,
                'DEC': 22.,
                'src_name': '1E 1740.7-2942',
            }
        )
        calculated_job_id = make_hash(restricted_par_dic)

        assert job_id == calculated_job_id



@pytest.mark.odaapi
@pytest.mark.test_catalog
@pytest.mark.parametrize("correct_format", [True, False])
def test_user_catalog_oda_api(dispatcher_live_fixture, correct_format):
    import oda_api.api
    import oda_api.data_products

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "unige-hpc-full, general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    if isinstance(encoded_token, bytes):
        encoded_token = encoded_token.decode()

    selected_catalog_dict = dict(
        cat_lon_name="ra",
        cat_lat_name="dec",
        cat_frame="fk5",
        cat_coord_units="deg",
        cat_column_list=[[1], ["Test A"], [6], [5], [4], [3], [2], [1], [0]],
        cat_column_names=["meta_ID", "src_names", "significance", "ra", "dec","NEW_SOURCE", "ISGRI_FLAG", "FLAG", "ERR_RAD"],
        cat_column_descr=[["meta_ID", "<i8"], ["src_names","<U6"], ["significance", "<i8"], ["ra", "<f8"], ["dec", "<f8"], ["NEW_SOURCE", "<i8"], ["ISGRI_FLAG","<i8"],["FLAG","<i8"],["ERR_RAD","<i8"]]
    )

    if not correct_format:
        selected_catalog_dict['cat_column_list'][8].append(0)

    disp = oda_api.api.DispatcherAPI(
        url=dispatcher_live_fixture,
        wait=False)
    get_product_args = dict(
        product_type="Dummy",
        instrument="empty",
        product="numerical",
        token=encoded_token,
        selected_catalog=json.dumps(selected_catalog_dict)
    )
    if not correct_format:
        with pytest.raises(oda_api.api.RequestNotUnderstood):
            disp.get_product(
                **get_product_args
            )
    else:
        prods = disp.get_product(
            **get_product_args
        )

        logger.info("product: %s", prods)
        logger.info("product show %s", prods.show())

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

        assert "selected_catalog" in jdata["prod_dictionary"]["analysis_parameters"]
        assert jdata["prod_dictionary"]["analysis_parameters"]["selected_catalog"] == json.dumps(selected_catalog_dict)

        # TODO the name of this method is misleading
        api_cat_dict = json.loads(prods.dispatcher_catalog_1.get_api_dictionary())
        assert api_cat_dict == selected_catalog_dict


def test_value_range(dispatcher_long_living_fixture):
    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "unige-hpc-full, general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    for is_ok, p in [
            (True, 10),
            (False, 1000)
        ]:

        params = {
            **default_params,
            'p': p,
            'product_type': 'numerical',
            'query_type': "Dummy",
            'instrument': 'empty',
            'token': encoded_token
        }

        if is_ok:
            expected_query_status = 'done'
            expected_job_status = 'done'
            expected_status_code = 200
        else:
            expected_query_status = None
            expected_job_status = None
            expected_status_code = 400

        logger.info("constructed server: %s", server)
        jdata = ask(server, params, expected_query_status=expected_query_status, expected_job_status=expected_job_status, max_time_s=50, expected_status_code=expected_status_code)
        logger.info(list(jdata.keys()))
        logger.info(jdata)

        if is_ok:
            pass
        else:
            assert jdata['error_message'] == 'p value is restricted to 800 W'


def test_empty_instrument_request(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
    }

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

    c = requests.get(server + "/run_analysis",
                     params=dict(
                       image_type="Real",
                       product_type="image",
                       E1_keV=20.,E2_keV=40.,
                       T1="2008-01-01T11:11:11.0",
                       T2="2008-06-01T11:11:11.0",
                     ))

    print("content:", c.text)

    assert c.status_code == 400


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

    example_config = yaml.load(open(example_config_fn), Loader=yaml.SafeLoader)['dispatcher']
    example_config.pop('product_gallery_options', None)

    mapper = lambda x, y: ".".join(map(str, x))
    example_config_keys = flatten_nested_structure(example_config, mapper)
    test_config_keys = flatten_nested_structure(dispatcher_test_conf, mapper)

    print("\n\n\nexample_config_keys", example_config_keys)
    print("\n\n\ntest_config_keys", test_config_keys)

    assert set(example_config_keys) == set(test_config_keys)


def test_example_config_with_gallery(dispatcher_test_conf_with_gallery):
    import cdci_data_analysis.config_dir

    example_config_fn = os.path.join(
        os.path.dirname(cdci_data_analysis.__file__),
        "config_dir/conf_env.yml.example"
    )

    example_config = yaml.load(open(example_config_fn), Loader=yaml.SafeLoader)['dispatcher']

    mapper = lambda x, y: ".".join(map(str, x))
    example_config_keys = flatten_nested_structure(example_config, mapper)
    test_config_keys = flatten_nested_structure(dispatcher_test_conf_with_gallery, mapper)

    print("\n\n\nexample_config_keys", example_config_keys)
    print("\n\n\ntest_config_keys", test_config_keys)

    assert set(example_config_keys) == set(test_config_keys)


def test_image(dispatcher_live_fixture):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "unige-hpc-full, general",
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

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=150,
                method='post',
                )

    assert 'numpy_data_product_list' in jdata['products']
    # test job_id
    job_id = jdata['products']['job_id']
    session_id = jdata['session_id']
    # adapting some values to string
    for k, v in params.items():
        params[k] = str(v)

    restricted_par_dic = InstrumentQueryBackEnd.restricted_par_dic(
        {
            **params,
            'p_list': [],
            'p': 55.,
            'RA': 83.0,
            'DEC': 22.0,
            'src_name': '1E 1740.7-2942',
            'sub': 'mtm@mtmco.net',
        }
    )
    calculated_job_id = make_hash(restricted_par_dic)

    assert job_id == calculated_job_id


@pytest.mark.parametrize("additional_parameter", [True, False])
def test_default_values(dispatcher_live_fixture, additional_parameter):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        'query_status': 'new',
        'product_type': 'numerical',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token,
    }

    if additional_parameter:
        params['additional_param'] = 'no_value'

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=150,
                )

    assert 'p' in jdata['products']['analysis_parameters']
    assert 'string_like_name' not in jdata['products']['analysis_parameters']
    if additional_parameter:
        assert 'additional_param' in jdata['products']['analysis_parameters']
    else:
        assert 'additional_param' not in jdata['products']['analysis_parameters']

    # test job_id
    job_id = jdata['products']['job_id']
    session_id = jdata['session_id']
    # adapting some values to string
    for k, v in params.items():
        params[k] = str(v)

    restricted_par_dic = InstrumentQueryBackEnd.restricted_par_dic({**params,
                                                                    'sub': 'mtm@mtmco.net',
                                                                    'p': 10.0,
                                                                    'p_list': [],
                                                                    'src_name': '1E 1740.7-2942',
                                                                    'RA': 265.97845833,
                                                                    'DEC': -29.74516667,
                                                                    'T1': '2017-03-06T13:26:48.000',
                                                                    'T2': '2017-03-06T15:32:27.000',
                                                                    'T_format': 'isot'
                                                                    })
    calculated_job_id = make_hash(restricted_par_dic)

    assert job_id == calculated_job_id

    # get the analysis_parameters json file
    analysis_parameters_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/analysis_parameters.json'
    # the aliased version might have been created
    analysis_parameters_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/analysis_parameters.json'
    assert os.path.exists(analysis_parameters_json_fn) or os.path.exists(analysis_parameters_json_fn_aliased)
    if os.path.exists(analysis_parameters_json_fn):
        analysis_parameters_json_content_original = json.load(open(analysis_parameters_json_fn))
    else:
        analysis_parameters_json_content_original = json.load(open(analysis_parameters_json_fn_aliased))

    assert 'p' in analysis_parameters_json_content_original
    assert 'string_like_name' not in analysis_parameters_json_content_original
    if additional_parameter:
        assert 'additional_param' in analysis_parameters_json_content_original
    else:
        assert 'additional_param' not in analysis_parameters_json_content_original

def test_empty_sentry(dispatcher_live_fixture_empty_sentry):
    server = dispatcher_live_fixture_empty_sentry

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
    }

    jdata = ask(server,
                params,
                expected_query_status=['done'],
                max_time_s=50,
                )
    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))

    assert jdata["exit_status"]["debug_message"] == ""
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == ""


@pytest.mark.xfail
def test_get_query_products_exception(dispatcher_live_fixture):
    # TODO this test will be re-inserted when refactoring the error propagation (https://github.com/oda-hub/dispatcher-app/issues/273)
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        'query_status': 'new',
        'product_type': 'failing',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token,
    }

    jdata = ask(server,
                params,
                expected_query_status='failed'
                )

    print("jdata : ", jdata)

    assert jdata['exit_status']['message'] == 'InternalError()\nfailing query\n'


@pytest.mark.test_drupal
@pytest.mark.parametrize("source_to_resolve", ['Mrk 421', 'Mrk_421', 'GX 1+4', 'fake object', None])
def test_source_resolver(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, source_to_resolve):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {'name': source_to_resolve,
              'token': encoded_token}

    c = requests.get(os.path.join(server, "resolve_name"),
                     params={**params}
                     )

    assert c.status_code == 200
    resolved_obj = c.json()
    print('Resolved object returned: ', resolved_obj)

    if source_to_resolve is None:
        assert resolved_obj == {}
    elif source_to_resolve == 'fake object':
        assert 'name' in resolved_obj
        assert 'message' in resolved_obj

        # the name resolver replaces automatically underscores with spaces in the returned name
        assert resolved_obj['name'] == source_to_resolve
        assert resolved_obj['message'] == f'{source_to_resolve} could not be resolved'
    else:
        assert 'name' in resolved_obj
        assert 'DEC' in resolved_obj
        assert 'RA' in resolved_obj
        assert 'entity_portal_link' in resolved_obj
        assert 'object_ids' in resolved_obj
        assert 'object_type' in resolved_obj

        assert resolved_obj['name'] == source_to_resolve.replace('_', ' ')
        assert resolved_obj['entity_portal_link'] == dispatcher_test_conf_with_gallery["product_gallery_options"]["entities_portal_url"]\
            .format(urllib.parse.quote(source_to_resolve.strip()))


@pytest.mark.test_drupal
@pytest.mark.parametrize("type_group", ['instruments', 'Instruments', 'products', 'sources', 'aaaaaa', '', None])
@pytest.mark.parametrize("parent", ['isgri', 'production', 'all', 'aaaaaa', '', None])
def test_list_terms(dispatcher_live_fixture_with_gallery, type_group, parent):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {'group': type_group,
              'parent': parent,
              'token': encoded_token}

    c = requests.get(os.path.join(server, "get_list_terms"),
                     params={**params}
                     )

    assert c.status_code == 200
    list_terms = c.json()
    print('List of terms returned: ', list_terms)
    assert isinstance(list_terms, list)
    if type_group is None or type_group == '' or type_group == 'aaaaaa' or \
            (type_group == 'products' and (parent == 'production' or parent == 'aaaaaa')):
        assert len(list_terms) == 0
    else:
        assert len(list_terms) > 0


@pytest.mark.test_drupal
@pytest.mark.parametrize("group", ['instruments', 'Instruments', 'products', '', 'aaaaaa', None])
@pytest.mark.parametrize("term", ['isgri', 'isgri_image', 'jemx_lc', 'aaaaaa', None])
def test_parents_term(dispatcher_live_fixture_with_gallery, term, group):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    params = {'term': term,
              'group': group,
              'token': encoded_token}

    c = requests.get(os.path.join(server, "get_parents_term"),
                     params={**params}
                     )

    assert c.status_code == 200
    list_terms = c.json()
    print('List of terms returned: ', list_terms)
    assert isinstance(list_terms, list)
    if term is None or term == 'aaaaaa' or \
            (term is not None and group == 'aaaaaa') or \
            ((term == 'jemx_lc' or term == 'isgri_image') and group is not None and str.lower(group) == 'instruments'):
        assert len(list_terms) == 0
    else:
        assert len(list_terms) > 0
        if term == 'jemx_lc':
            assert 'lightcurve' in list_terms
        elif term == 'isgri_image':
            assert 'image' in list_terms
        elif term == 'isgri':
            if group == 'products':
                assert 'instruments' in list_terms
            elif group == 'instruments':
                assert 'production' in list_terms
  
 
@pytest.mark.test_drupal
@pytest.mark.parametrize("time_to_convert", ['2022-03-29T15:51:01', '', 'aaaaaa', None])
def test_converttime_revnum(dispatcher_live_fixture_with_gallery, time_to_convert):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {'time_to_convert': time_to_convert,
              'token': encoded_token}

    c = requests.get(os.path.join(server, "get_revnum"),
                     params={**params}
                     )
    assert c.status_code == 200
    revnum_obj = c.json()
    print('Rev number returned: ', revnum_obj)
    if time_to_convert == 'aaaaaa':
        assert revnum_obj == {}
    else:
        assert 'revnum' in revnum_obj
        if time_to_convert == '2022-03-29T15:51:01':
            assert revnum_obj['revnum'] == 2485


@pytest.mark.test_drupal
@pytest.mark.parametrize("obsid", [1960001, ["1960001", "1960002", "1960003"]])
@pytest.mark.parametrize("timerange_parameters", ["time_range_no_timezone", "time_range_no_timezone_limits", "time_range_with_timezone", "new_time_range", "observation_id"])
def test_product_gallery_data_product_with_period_of_observation(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, timerange_parameters, obsid):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        'content_type': 'data_product',
        'product_title': 'Test observation range',
        'token': encoded_token,
        'obsid': obsid
    }
    if isinstance(obsid, list):
        params['obsid'] = ','.join(obsid)

    file_obj = {'yaml_file_0': open('observation_yaml_dummy_files/obs_rev_2542.yaml', 'rb')}

    now = datetime.now()

    if timerange_parameters == 'time_range_no_timezone':
        params['T1'] = '2022-07-21T00:29:47'
        params['T2'] = '2022-07-23T05:29:11'
    elif timerange_parameters == 'time_range_no_timezone_limits':
        params['T1'] = '2021-02-01T00:00:00'
        params['T2'] = '2021-03-31T23:59:59'
    elif timerange_parameters == 'time_range_with_timezone':
        params['T1'] = '2022-07-21T00:29:47+0100'
        params['T2'] = '2022-07-23T05:29:11+0100'
    elif timerange_parameters == 'observation_id':
        params['observation_id'] = 'test observation'
    elif timerange_parameters == 'new_time_range':
        params['T1'] = (now - timedelta(days=random.randint(30, 150))).strftime('%Y-%m-%dT%H:%M:%S')
        params['T2'] = now.strftime('%Y-%m-%dT%H:%M:%S')

    c = requests.post(os.path.join(server, "post_product_to_gallery"),
                      params={**params},
                      files=file_obj
                      )

    assert c.status_code == 200

    drupal_res_obj = c.json()

    link_field_derived_from_observation = os.path.join(
        dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
        'rest/relation/node/data_product/field_derived_from_observation')
    assert link_field_derived_from_observation in drupal_res_obj['_links']
    parsed_link_field_derived_from_observation = parse.urlparse(drupal_res_obj['_links'][link_field_derived_from_observation][0]['href']).path.split('/')[-1]

    link_obs = os.path.join(
        dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
        f'node/{parsed_link_field_derived_from_observation}?_format=hal_json')

    user_id_product_creator = get_user_id(product_gallery_url=dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                                          user_email=token_payload['sub'])
    gallery_jwt_token = generate_gallery_jwt_token(dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_secret_key'],
                                                   user_id=user_id_product_creator)

    header_request = get_drupal_request_headers(gallery_jwt_token)
    response_obs_info = execute_drupal_request(link_obs, headers=header_request)

    drupal_res_obs_info_obj = response_obs_info.json()

    assert 'field_timerange' in drupal_res_obs_info_obj
    obs_per_field_timerange = drupal_res_obs_info_obj['field_timerange']
    obs_per_title = drupal_res_obs_info_obj['title'][0]['value']

    assert 'field_obsid' in drupal_res_obs_info_obj
    if isinstance(obsid, list):
        for single_obsid in obsid:
            assert drupal_res_obs_info_obj['field_obsid'][obsid.index(single_obsid)]['value'] == single_obsid
    else:
        assert drupal_res_obs_info_obj['field_obsid'][0]['value'] == str(obsid)

    link_field_field_attachments = os.path.join(
        dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
        'rest/relation/node/observation/field_attachments')
    assert link_field_field_attachments in drupal_res_obs_info_obj['_links']

    obs_per_field_timerange_start_no_timezone = parser.parse(obs_per_field_timerange[0]['value']).strftime('%Y-%m-%dT%H:%M:%S')
    obs_per_field_timerange_end_no_timezone = parser.parse(obs_per_field_timerange[0]['end_value']).strftime(
        '%Y-%m-%dT%H:%M:%S')

    if timerange_parameters in ['time_range_no_timezone', 'time_range_with_timezone', 'new_time_range', 'time_range_no_timezone_limits']:
        parsed_t1_no_timezone = parser.parse(params['T1']).strftime('%Y-%m-%dT%H:%M:%S')
        parsed_t2_no_timezone = parser.parse(params['T2']).strftime('%Y-%m-%dT%H:%M:%S')
        assert obs_per_field_timerange_start_no_timezone == parsed_t1_no_timezone
        assert obs_per_field_timerange_end_no_timezone == parsed_t2_no_timezone
        if timerange_parameters == 'new_time_range':
            assert 'field_rev1' in drupal_res_obs_info_obj
            assert 'field_rev2' in drupal_res_obs_info_obj
            revnum1_input = get_revnum(service_url=dispatcher_test_conf_with_gallery['product_gallery_options']['converttime_revnum_service_url'],
                                       time_to_convert=params['T1'])
            assert drupal_res_obs_info_obj['field_rev1'][0]['value'] == revnum1_input['revnum']
            revnum2_input = get_revnum(service_url=dispatcher_test_conf_with_gallery['product_gallery_options']['converttime_revnum_service_url'],
                                       time_to_convert=params['T2'])
            assert drupal_res_obs_info_obj['field_rev2'][0]['value'] == revnum2_input['revnum']
            # additional check for the time range REST call
            observations_range = get_observations_for_time_range(dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                                                                 gallery_jwt_token,
                                                                 t1=params['T1'], t2=params['T2'])
            assert len(observations_range) == 1
            times = observations_range[0]['field_timerange'].split('--')
            t_start = parser.parse(times[0]).strftime('%Y-%m-%dT%H:%M:%S')
            t_end = parser.parse(times[1]).strftime('%Y-%m-%dT%H:%M:%S')
            assert parsed_t1_no_timezone == t_start
            assert parsed_t2_no_timezone == t_end
    else:
        assert obs_per_title == 'test observation'


@pytest.mark.xfail
@pytest.mark.test_drupal
def test_product_gallery_post_period_of_observation_with_revnum(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    t1 = '2022-07-21T00:29:47'
    t2 = '2022-07-23T05:29:11'
    revnum1_input = get_revnum(
        service_url=dispatcher_test_conf_with_gallery['product_gallery_options']['converttime_revnum_service_url'],
        time_to_convert=t1)
    revnum2_input = get_revnum(
        service_url=dispatcher_test_conf_with_gallery['product_gallery_options']['converttime_revnum_service_url'],
        time_to_convert=t2)

    params = {
        'token': encoded_token,
        'title': 'test observation title with rev num',
        'revnum_1': revnum1_input['revnum'],
        'revnum_2': revnum2_input['revnum']
    }

    c = requests.post(os.path.join(server, "post_observation_to_gallery"),
                      params={**params},
                      )

    assert c.status_code == 200

    drupal_res_obj = c.json()

    observation_id = drupal_res_obj['nid'][0]['value']

    link_obs = os.path.join(
        dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
        f'node/{observation_id}?_format=hal_json')

    user_id_product_creator = get_user_id(product_gallery_url=dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                                          user_email=token_payload['sub'])
    gallery_jwt_token = generate_gallery_jwt_token(dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_secret_key'],
                                                   user_id=user_id_product_creator)

    header_request = get_drupal_request_headers(gallery_jwt_token)
    response_obs_info = execute_drupal_request(link_obs, headers=header_request)

    drupal_res_obs_info_obj = response_obs_info.json()

    # assert 'field_timerange' in drupal_res_obs_info_obj
    # obs_per_field_timerange = drupal_res_obs_info_obj['field_timerange']
    # obs_per_field_timerange_start_no_timezone = parser.parse(obs_per_field_timerange[0]['value']).strftime('%Y-%m-%dT%H:%M:%S')
    # obs_per_field_timerange_end_no_timezone = parser.parse(obs_per_field_timerange[0]['end_value']).strftime(
    #     '%Y-%m-%dT%H:%M:%S')
    # assert obs_per_field_timerange_start_no_timezone == t1
    # assert obs_per_field_timerange_end_no_timezone == t2

    assert 'field_rev1' in drupal_res_obs_info_obj
    assert 'field_rev2' in drupal_res_obs_info_obj
    assert drupal_res_obs_info_obj['field_rev1'][0]['value'] == revnum1_input['revnum']
    assert drupal_res_obs_info_obj['field_rev2'][0]['value'] == revnum2_input['revnum']


@pytest.mark.test_drupal
@pytest.mark.parametrize("force_creation_new", [True, False])
def test_product_gallery_update_new_astrophysical_entity(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, force_creation_new):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        'token': encoded_token,
        'src_name': 'test astro entity' + '_' + str(uuid.uuid4()),
        'update_astro_entity': True,
        'create_new': force_creation_new
    }

    c = requests.post(os.path.join(server, "post_astro_entity_to_gallery"),
                      params={**params},
                      )

    assert c.status_code == 200

    drupal_res_obj = c.json()

    if force_creation_new:
        assert drupal_res_obj['title'][0]['value'] == params['src_name']
    else:
        assert drupal_res_obj == {}


@pytest.mark.test_drupal
@pytest.mark.parametrize("auto_update", [True, False])
def test_product_gallery_update_existing_astrophysical_entity(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, auto_update):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        'token': encoded_token,
        'src_name': 'GX 1+4',
        'source_dec': -24,
        'update_astro_entity': True
    }

    c = requests.post(os.path.join(server, "post_astro_entity_to_gallery"),
                      params={**params},
                      )

    assert c.status_code == 200
    drupal_res_obj = c.json()
    assert drupal_res_obj['field_source_dec'][0]['value'] == params['source_dec']

    params = {
        'token': encoded_token,
        'src_name': 'GX 1+4',
        'source_dec': -24.9,
        'update_astro_entity': True,
        'auto_update': auto_update
    }

    c = requests.post(os.path.join(server, "post_astro_entity_to_gallery"),
                      params={**params},
                      )

    assert c.status_code == 200
    drupal_res_obj = c.json()
    if auto_update:
        assert drupal_res_obj['field_source_dec'][0]['value'] != params['source_dec']
    else:
        assert drupal_res_obj['field_source_dec'][0]['value'] == params['source_dec']


@pytest.mark.test_drupal
def test_product_gallery_get_period_of_observation_attachments(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    now = datetime.now()

    params = {
        'token': encoded_token,
        'obsid': "1960001, 1960002, 1960003",
        'title': "test observation title",
        'T1': (now - timedelta(days=random.randint(30, 150))).strftime('%Y-%m-%dT%H:%M:%S'),
        'T2': now.strftime('%Y-%m-%dT%H:%M:%S')
    }

    file_obj = {'yaml_file_0': open('observation_yaml_dummy_files/obs_rev_2542.yaml', 'rb'),
                'yaml_file_1': open('observation_yaml_dummy_files/obs_rev_1.yaml', 'rb')}

    c = requests.post(os.path.join(server, "post_observation_to_gallery"),
                      params={**params},
                      files=file_obj
                      )

    assert c.status_code == 200

    c = requests.get(os.path.join(server, "get_observation_attachments"),
                     params={'title': 'test observation title',
                             'token': encoded_token}
                     )

    assert c.status_code == 200
    drupal_res_obj = c.json()

    assert 'file_path' in drupal_res_obj
    assert 'file_content' in drupal_res_obj

    with open('observation_yaml_dummy_files/obs_rev_2542.yaml', 'r') as f_yaml_file_yaml_file_content_obs_rev_2542:
        yaml_file_content_obs_rev_2542 = f_yaml_file_yaml_file_content_obs_rev_2542.read()

    with open('observation_yaml_dummy_files/obs_rev_1.yaml', 'r') as f_yaml_file_yaml_file_content_obs_rev_1:
        yaml_file_content_obs_rev_1 = f_yaml_file_yaml_file_content_obs_rev_1.read()

    assert yaml_file_content_obs_rev_1 in drupal_res_obj['file_content']
    assert yaml_file_content_obs_rev_2542 in drupal_res_obj['file_content']


@pytest.mark.test_drupal
@pytest.mark.parametrize("obsid", [1960001, ["1960001", "1960002", "1960003"]])
@pytest.mark.parametrize("timerange_parameters", ["time_range_no_timezone", "time_range_no_timezone_limits", "time_range_with_timezone", "new_time_range"])
@pytest.mark.parametrize("include_title", [True, False])
def test_product_gallery_post_period_of_observation(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, timerange_parameters, obsid, include_title):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        'token': encoded_token,
        'obsid': obsid
    }
    if isinstance(obsid, list):
        params['obsid'] = ','.join(obsid)
    if include_title:
        params['title'] = "test observation title"

    file_obj = {'yaml_file_0': open('observation_yaml_dummy_files/obs_rev_2542.yaml', 'rb')}

    now = datetime.now()

    if timerange_parameters == 'time_range_no_timezone':
        params['T1'] = '2022-07-21T00:29:47'
        params['T2'] = '2022-07-23T05:29:11'
    elif timerange_parameters == 'time_range_no_timezone_limits':
        params['T1'] = '2021-02-01T00:00:00'
        params['T2'] = '2021-03-31T23:59:59'
    elif timerange_parameters == 'time_range_with_timezone':
        params['T1'] = '2022-07-21T00:29:47+0100'
        params['T2'] = '2022-07-23T05:29:11+0100'
    elif timerange_parameters == 'new_time_range':
        params['T1'] = (now - timedelta(days=random.randint(30, 150))).strftime('%Y-%m-%dT%H:%M:%S')
        params['T2'] = now.strftime('%Y-%m-%dT%H:%M:%S')

    c = requests.post(os.path.join(server, "post_observation_to_gallery"),
                      params={**params},
                      files=file_obj
                      )

    assert c.status_code == 200

    drupal_res_obj = c.json()

    observation_id = drupal_res_obj['nid'][0]['value']

    link_obs = os.path.join(
        dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
        f'node/{observation_id}?_format=hal_json')

    user_id_product_creator = get_user_id(product_gallery_url=dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                                          user_email=token_payload['sub'])
    gallery_jwt_token = generate_gallery_jwt_token(dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_secret_key'],
                                                   user_id=user_id_product_creator)

    header_request = get_drupal_request_headers(gallery_jwt_token)
    response_obs_info = execute_drupal_request(link_obs, headers=header_request)

    drupal_res_obs_info_obj = response_obs_info.json()

    assert 'field_timerange' in drupal_res_obs_info_obj
    obs_per_field_timerange = drupal_res_obs_info_obj['field_timerange']

    assert 'field_obsid' in drupal_res_obs_info_obj
    if isinstance(obsid, list):
        for single_obsid in obsid:
            assert drupal_res_obs_info_obj['field_obsid'][obsid.index(single_obsid)]['value'] == single_obsid
    else:
        assert drupal_res_obs_info_obj['field_obsid'][0]['value'] == str(obsid)

    link_field_field_attachments = os.path.join(
        dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
        'rest/relation/node/observation/field_attachments')
    assert link_field_field_attachments in drupal_res_obs_info_obj['_links']

    obs_per_field_timerange_start_no_timezone = parser.parse(obs_per_field_timerange[0]['value']).strftime('%Y-%m-%dT%H:%M:%S')
    obs_per_field_timerange_end_no_timezone = parser.parse(obs_per_field_timerange[0]['end_value']).strftime(
        '%Y-%m-%dT%H:%M:%S')

    parsed_t1_no_timezone = parser.parse(params['T1']).strftime('%Y-%m-%dT%H:%M:%S')
    parsed_t2_no_timezone = parser.parse(params['T2']).strftime('%Y-%m-%dT%H:%M:%S')
    assert obs_per_field_timerange_start_no_timezone == parsed_t1_no_timezone
    assert obs_per_field_timerange_end_no_timezone == parsed_t2_no_timezone
    if timerange_parameters == 'new_time_range':
        assert 'field_rev1' in drupal_res_obs_info_obj
        assert 'field_rev2' in drupal_res_obs_info_obj
        assert 'field_span_rev' in drupal_res_obs_info_obj
        revnum1_input = get_revnum(service_url=dispatcher_test_conf_with_gallery['product_gallery_options']['converttime_revnum_service_url'],
                                   time_to_convert=params['T1'])
        assert drupal_res_obs_info_obj['field_rev1'][0]['value'] == revnum1_input['revnum']
        revnum2_input = get_revnum(service_url=dispatcher_test_conf_with_gallery['product_gallery_options']['converttime_revnum_service_url'],
                                   time_to_convert=params['T2'])
        assert drupal_res_obs_info_obj['field_rev2'][0]['value'] == revnum2_input['revnum']
        # additional check for the time range REST call
        observations_range = get_observations_for_time_range(dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                                                             gallery_jwt_token,
                                                             t1=params['T1'], t2=params['T2'])
        assert drupal_res_obs_info_obj['field_span_rev'][0]['value'] == revnum2_input['revnum'] - revnum1_input['revnum']
        assert len(observations_range) == 1
        times = observations_range[0]['field_timerange'].split('--')
        t_start = parser.parse(times[0]).strftime('%Y-%m-%dT%H:%M:%S')
        t_end = parser.parse(times[1]).strftime('%Y-%m-%dT%H:%M:%S')
        assert parsed_t1_no_timezone == t_start
        assert parsed_t2_no_timezone == t_end

    if include_title:
        assert drupal_res_obs_info_obj['title'][0]['value'] == params['title']


@pytest.mark.test_drupal
@pytest.mark.parametrize("provide_job_id", [True, False])
@pytest.mark.parametrize("provide_instrument", [True, False])
@pytest.mark.parametrize("provide_product_type", [True, False])
@pytest.mark.parametrize("timerange_parameters", ["time_range", "observation_id", None])
@pytest.mark.parametrize("type_source", ["known", "new", None])
@pytest.mark.parametrize("insert_new_source", [True, False])
@pytest.mark.parametrize("provide_product_title", [True, False])
def test_product_gallery_post(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, provide_job_id, provide_instrument, provide_product_type, timerange_parameters, type_source, insert_new_source, provide_product_title):
    dispatcher_fetch_dummy_products('default')

    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # send simple request
    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    instrument = 'empty'
    product_type_analysis = 'numerical'

    params = {
        **default_params,
        'src_name': 'Mrk 421',
        'product_type': product_type_analysis,
        'query_type': "Dummy",
        'instrument': instrument,
        'p': 5,
        'token': encoded_token
    }

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=150,
                )

    job_id = jdata['products']['job_id']

    e1_kev = 45
    e2_kev = 95

    dec = 19
    ra = 458

    source_name = None
    if type_source == "known":
        source_name = "Crab"
    elif type_source == "new":
        source_name = "new_source_" + ''.join(random.choices(string.digits + string.ascii_lowercase, k=5))

    product_title = None
    if provide_product_title:
        product_title = "_".join([params['instrument'], params['query_type'], datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")])

    if not provide_job_id:
        job_id = None
    if not provide_instrument:
        instrument = None
    else:
        # a difference value
        instrument = 'isgri'
    if not provide_product_type:
        product_type_product_gallery = None
    else:
        product_type_product_gallery = 'isgri_lc'

    params = {
        'job_id': job_id,
        'instrument': instrument,
        'src_name': source_name,
        'product_type': product_type_product_gallery,
        'content_type': 'data_product',
        'product_title': product_title,
        'E1_keV': e1_kev,
        'E2_kev': e2_kev,
        'DEC': dec,
        'RA': ra,
        'token': encoded_token,
        'insert_new_source': insert_new_source
    }
    if timerange_parameters == 'time_range':
        params['T1'] = '2003-03-15T23:27:40.0'
        params['T2'] = '2003-03-16T00:03:12.0'
    elif timerange_parameters == 'observation_id':
        params['observation_id'] = 'test observation'

    # send test img and test fits file
    file_obj = {'img': open('data/dummy_prods/ds9.jpeg', 'rb'),
                'fits_file_0': open('data/dummy_prods/isgri_query_lc.fits', 'rb'),
                'fits_file_1': open('data/dummy_prods/query_catalog.fits', 'rb')}

    c = requests.post(os.path.join(server, "post_product_to_gallery"),
                      params={**params},
                      files=file_obj
                      )

    assert c.status_code == 200

    drupal_res_obj = c.json()

    if not provide_product_title:
        if provide_product_type and type_source is not None:
            product_title = "_".join([source_name, product_type_product_gallery])
        elif provide_product_type and type_source is None:
            product_title = product_type_product_gallery
        elif not provide_product_type and type_source is not None:
            if provide_job_id:
                product_title = "_".join([source_name, product_type_analysis])
            else:
                product_title = source_name
        elif not provide_product_type and type_source is None:
            if provide_job_id:
                product_title = product_type_analysis
            else:
                product_title = None

    assert 'title' in drupal_res_obj
    if product_title is not None:
        assert drupal_res_obj['title'][0]['value'] == product_title
    else:
        assert drupal_res_obj['title'][0]['value'].startswith('data_product_')

    assert 'field_e1_kev' in drupal_res_obj
    assert drupal_res_obj['field_e1_kev'][0]['value'] == e1_kev

    assert 'field_e2_kev' in drupal_res_obj
    assert drupal_res_obj['field_e2_kev'][0]['value'] == e2_kev

    assert 'field_dec' in drupal_res_obj
    assert drupal_res_obj['field_dec'][0]['value'] == dec

    assert 'field_ra' in drupal_res_obj
    assert drupal_res_obj['field_ra'][0]['value'] == ra

    if provide_instrument or (provide_job_id):
        link_field_instrumentused = os.path.join(dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                                                 'rest/relation/node/data_product/field_instrumentused')
        assert link_field_instrumentused in drupal_res_obj['_links']

    if provide_product_type or (provide_job_id):
        link_field_data_product_type = os.path.join(dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                                                 'rest/relation/node/data_product/field_data_product_type')
        assert link_field_data_product_type in drupal_res_obj['_links']

    link_field_derived_from_observation = os.path.join(
        dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
        'rest/relation/node/data_product/field_derived_from_observation')
    if timerange_parameters is not None or (provide_job_id):
        assert link_field_derived_from_observation in drupal_res_obj['_links']
    else:
        assert link_field_derived_from_observation not in drupal_res_obj['_links']


@pytest.mark.test_drupal
@pytest.mark.parametrize("type_source", ["single", "list", None])
@pytest.mark.parametrize("insert_new_source", [True])
def test_post_data_product_with_multiple_sources(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, type_source, insert_new_source):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # send simple request
    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    source_name = None
    entity_portal_link = None
    object_ids = None
    object_type = None
    source_coord = None
    if type_source == "single":
        source_name = "GX 1+4"
        entity_portal_link = "http://cdsportal.u-strasbg.fr/?target=GX%201%204"
        object_ids = [["GX 1+4", "GX 99", "Test"]]
        object_type = ["Symbiotic"]
        source_coord = [{"source_ra": 263.00897166666664, "source_dec": -24.74559138888889}]
    elif type_source == "list":
        source_name = 'GX 1+4, Crab, unknown_src, unknown_src_no_link'
        entity_portal_link = "http://cdsportal.u-strasbg.fr/?target=GX%201%204, http://cdsportal.u-strasbg.fr/?target=Crab, , link"
        object_ids = [["GX 1+4", "GX 99", "Test"], ["Crab", "GX 99", "Test"], [], ["unknown_src_no_link", "unknown source 1", "unknown source 2", "unknown source 3", "GX 1+4"]]
        object_type = ["Symbiotic", "SNRemnant", "", "Test"]
        source_coord = [{"source_ra": 263.00897166666664, "source_dec": -24.74559138888889},
                        {"source_ra": 83.63333333333331, "source_dec": 22.013333333333332},
                        {},
                        {"source_ra": 11.11, "source_dec": 43.89}]

    params = {
        'instrument': 'isgri',
        'src_name': source_name,
        'entity_portal_link_list': entity_portal_link,
        'object_ids_list': json.dumps(object_ids),
        'source_coord_list': json.dumps(source_coord),
        'object_type_list': json.dumps(object_type),
        'product_type': 'isgri_lc',
        'content_type': 'data_product',
        'product_title': "product with multiple sources",
        'token': encoded_token,
        'insert_new_source': insert_new_source
    }
    c = requests.post(os.path.join(server, "post_product_to_gallery"),
                      params={**params}
                      )

    assert c.status_code == 200

    drupal_res_obj = c.json()

    link_field_describes_astro_entity = os.path.join(
        dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
        'rest/relation/node/data_product/field_describes_astro_entity')
    if type_source is not None:
        assert link_field_describes_astro_entity in drupal_res_obj['_links']
        if type_source == "single":
            assert len(drupal_res_obj['_links'][link_field_describes_astro_entity]) == 1
        elif type_source == "list":
            assert len(drupal_res_obj['_links'][link_field_describes_astro_entity]) == len(source_name.split(','))
            user_id_product_creator = get_user_id(
                product_gallery_url=dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                user_email=token_payload['sub'])
            gallery_jwt_token = generate_gallery_jwt_token(
                dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_secret_key'],
                user_id=user_id_product_creator)
            source_entity_id = get_source_astrophysical_entity_id_by_source_name(
                dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                gallery_jwt_token,
                source_name="unknown_src_no_link")
            assert source_entity_id is not None

            link_source = os.path.join(
                dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                f'node/{source_entity_id}?_format=hal_json')

            header_request = get_drupal_request_headers(gallery_jwt_token)
            response_obs_info = execute_drupal_request(link_source, headers=header_request)

            drupal_res_source_info_obj = response_obs_info.json()

            assert 'field_alternative_names_long_str' in drupal_res_source_info_obj
            field_alternative_names_long_str_splitted = drupal_res_source_info_obj['field_alternative_names_long_str'][0]['value'].split(',')
            assert len(field_alternative_names_long_str_splitted) == 5
            assert field_alternative_names_long_str_splitted[0] == 'unknown_src_no_link'
            assert field_alternative_names_long_str_splitted[1] == 'unknown source 1'
            assert field_alternative_names_long_str_splitted[2] == 'unknown source 2'
            assert field_alternative_names_long_str_splitted[3] == 'unknown source 3'
            assert field_alternative_names_long_str_splitted[4] == 'GX 1+4'

            assert 'field_source_ra' in drupal_res_source_info_obj
            assert drupal_res_source_info_obj['field_source_ra'][0]['value'] == source_coord[3]['source_ra']
            assert 'field_source_dec' in drupal_res_source_info_obj
            assert drupal_res_source_info_obj['field_source_dec'][0]['value'] == source_coord[3]['source_dec']
            assert 'field_link' in drupal_res_source_info_obj
            assert drupal_res_source_info_obj['field_link'][0]['value'] == 'link'
            assert 'field_object_type' in drupal_res_source_info_obj
            assert drupal_res_source_info_obj['field_object_type'][0]['value'] == 'Test'

    else:
        assert link_field_describes_astro_entity not in drupal_res_obj['_links']


@pytest.mark.test_drupal
def test_product_gallery_update(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery):
    dispatcher_fetch_dummy_products('default')

    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # send simple request
    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    instrument = 'empty'
    product_type_analysis = 'numerical'

    params = {
        **default_params,
        'src_name': 'Mrk 421',
        'product_type': product_type_analysis,
        'query_type': "Dummy",
        'instrument': instrument,
        'p': 5,
        'token': encoded_token
    }

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=150,
                )

    job_id = jdata['products']['job_id']

    e1_kev = 45
    e2_kev = 95

    dec = 19
    ra = 458

    source_name = "Crab"
    instrument = 'isgri'
    product_type_product_gallery = 'isgri_lc'

    params = {
        'product_id': job_id,
        'instrument': instrument,
        'src_name': source_name,
        'product_type': product_type_product_gallery,
        'content_type': 'data_product',
        'E1_keV': e1_kev,
        'E2_kev': e2_kev,
        'DEC': dec,
        'RA': ra,
        'token': encoded_token
    }

    params['product_title'] = "_".join([params['instrument'], params['product_type'],
                              datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")])

    params['T1'] = '2003-03-15T23:27:40.0'
    params['T2'] = '2003-03-16T00:03:12.0'

    # send test img and test fits file
    file_obj = {'img': open('data/dummy_prods/ds9.jpeg', 'rb'),
                'fits_file_0': open('data/dummy_prods/isgri_query_lc.fits', 'rb'),
                'fits_file_1': open('data/dummy_prods/query_catalog.fits', 'rb')}

    c = requests.post(os.path.join(server, "post_product_to_gallery"),
                      params={**params},
                      files=file_obj
                      )

    assert c.status_code == 200

    drupal_res_obj = c.json()

    assert 'nid' in drupal_res_obj
    nid_creation = drupal_res_obj['nid'][0]['value']

    assert 'field_e1_kev' in drupal_res_obj
    assert drupal_res_obj['field_e1_kev'][0]['value'] == e1_kev

    assert 'field_e2_kev' in drupal_res_obj
    assert drupal_res_obj['field_e2_kev'][0]['value'] == e2_kev

    link_img_id = os.path.join(dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                               'rest/relation/node/data_product/field_image_png')

    assert link_img_id in drupal_res_obj['_links']
    assert len(drupal_res_obj['_links'][link_img_id]) == 1

    link_fits_file_id = os.path.join(dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
                                     'rest/relation/node/data_product/field_fits_file')
    assert link_fits_file_id in drupal_res_obj['_links']
    assert len(drupal_res_obj['_links'][link_fits_file_id]) == 2

    id_posted_data_product = drupal_res_obj['nid'][0]['value']

    params = {
        'e1_kev': 145,
        'e2_kev': 195,
        'product_id': job_id,
        'content_type': 'data_product',
        'token': encoded_token
    }

    params['T1'] = '2003-03-15T23:27:40.0'
    params['T2'] = '2003-03-16T00:03:12.0'

    # send test img and test fits file
    file_obj = {'img': open('data/dummy_prods/ds9.jpeg', 'rb'),
                'fits_file_0': open('data/dummy_prods/isgri_query_lc.fits', 'rb')}

    c = requests.post(os.path.join(server, "post_product_to_gallery"),
                      params={**params},
                      files=file_obj
                      )
    assert c.status_code == 200

    drupal_res_obj = c.json()

    assert 'nid' in drupal_res_obj
    nid_update = drupal_res_obj['nid'][0]['value']
    assert nid_update == nid_creation

    assert 'field_e1_kev' in drupal_res_obj
    assert drupal_res_obj['field_e1_kev'][0]['value'] == params['e1_kev']

    assert 'field_e2_kev' in drupal_res_obj
    assert drupal_res_obj['field_e2_kev'][0]['value'] == params['e2_kev']

    assert drupal_res_obj['nid'][0]['value'] == id_posted_data_product

    assert link_img_id in drupal_res_obj['_links']
    assert len(drupal_res_obj['_links'][link_img_id]) == 1

    assert link_fits_file_id in drupal_res_obj['_links']
    assert len(drupal_res_obj['_links'][link_fits_file_id]) == 1



@pytest.mark.test_drupal
def test_product_gallery_error_message(dispatcher_live_fixture_with_gallery):
    dispatcher_fetch_dummy_products('default')

    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # send simple request
    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    e1_kev = 45
    e2_kev = 95

    params = {
        'content_type': 'data_product',
        'E1_keV': e1_kev,
        'E2_kev': e2_kev,
        'E3_kev': 123,
        'token': encoded_token
    }

    c = requests.post(os.path.join(server, "post_product_to_gallery"),
                      params={**params},
                      )

    assert c.status_code == 500

    drupal_res_obj = c.json()

    assert 'drupal_helper_error_message' in drupal_res_obj
    assert 'InvalidArgumentException: Field field_e3_kev is unknown.' \
           in drupal_res_obj['drupal_helper_error_message']


@pytest.mark.test_renku
@pytest.mark.parametrize("existing_branch", [True, False])
@pytest.mark.parametrize("scw_list_passage", ['file', 'params'])
def test_posting_renku(dispatcher_live_fixture_with_renku_options, dispatcher_test_conf_with_renku_options, existing_branch, scw_list_passage):
    server = dispatcher_live_fixture_with_renku_options
    print("constructed server:", server)
    logger.info("constructed server: %s", server)

    # send simple request
    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, renku contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    p = 7

    if not existing_branch:
        p += random.random()
    params = {
        **default_params,
        'src_name': 'Mrk 421',
        'product_type': 'numerical',
        'query_type': "Dummy",
        'instrument': 'empty',
        'p': p,
        'token': encoded_token
    }

    if scw_list_passage == 'file':
        params['use_scws'] = 'user_file'
        file_path = DispatcherJobState.create_p_value_file(p_value=5)
        list_file = open(file_path)

        jdata = ask(server,
                    params,
                    expected_query_status=["done"],
                    max_time_s=150,
                    method='post',
                    files={'user_scw_list_file': list_file.read()}
                    )

        list_file.close()
    elif scw_list_passage == 'params':
        params['scw_list'] = [f"0665{i:04d}0010.001" for i in range(50)]
        params['use_scws'] = 'form_list'
        jdata = ask(server,
                    params,
                    expected_query_status=["done"],
                    max_time_s=150
                    )

    job_id = jdata['products']['job_id']
    session_id = jdata['products']['session_id']
    request_dict = jdata['products']['analysis_parameters']
    params = {
        'job_id': job_id,
        'token': encoded_token
    }
    c = requests.post(os.path.join(server, "push-renku-branch"),
                      params={**params}
                      )

    assert c.status_code == 200

    # parse the repo url and build the renku one
    products_url = dispatcher_test_conf_with_renku_options['products_url']
    repo_url = dispatcher_test_conf_with_renku_options['renku_options']['renku_gitlab_repository_url']
    renku_base_project_url = dispatcher_test_conf_with_renku_options['renku_options']['renku_base_project_url']
    renku_gitlab_ssh_key_path = dispatcher_test_conf_with_renku_options['renku_options']['ssh_key_path']
    repo_path = get_repo_path(repo_url)
    renku_project_url = f'{renku_base_project_url}/{repo_path}'


    # validate content pushed
    repo = clone_renku_repo(repo_url, renku_gitlab_ssh_key_path=renku_gitlab_ssh_key_path)

    assert check_job_id_branch_is_present(repo, job_id)

    repo = checkout_branch_renku_repo(repo, branch_name=f'mmoda_request_{job_id}')
    repo.git.pull("--set-upstream", repo.remote().name, str(repo.head.ref))
    api_code_file_name = generate_notebook_filename(job_id=job_id)

    assert c.text == f"{renku_project_url}/sessions/new?autostart=1&branch=mmoda_request_{job_id}&commit={repo.head.commit.hexsha}&notebook={api_code_file_name}"

    api_code_file_path = os.path.join(repo.working_dir, api_code_file_name)

    extracted_api_code = DispatcherJobState.extract_api_code(session_id, job_id)
    token_pattern = r"[\'\"]token[\'\"]:\s*?[\'\"].*?[\'\"]"
    extracted_api_code = re.sub(token_pattern, '# "token": getpass.getpass(),', extracted_api_code, flags=re.DOTALL)

    extracted_api_code = 'import getpass\n\n' + extracted_api_code

    assert os.path.exists(api_code_file_path)
    parsed_notebook = nbf.read(api_code_file_path, 4)
    assert len(parsed_notebook.cells) == 2
    assert parsed_notebook.cells[0].source == "# Notebook automatically generated from MMODA"
    assert parsed_notebook.cells[1].source == extracted_api_code

    assert repo.head.reference.commit.message is not None
    request_url = generate_commit_request_url(products_url, request_dict)
    commit_message = (f"Stored API code of MMODA request by {token_payload['name']} for a {request_dict['product_type']}"
                      f" from the instrument {request_dict['instrument']}"
                      f"\nthe original request was generated via {request_url}\n"
                      "to retrieve the result please follow the link")
    assert repo.head.reference.commit.message == commit_message

    shutil.rmtree(repo.working_dir)


@pytest.mark.fast
def test_param_value(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    c = requests.get(server + "/run_analysis",
                   params={'instrument': 'empty',
                           'product_type': 'echo',
                           'query_status': 'new',
                           'query_type': 'Real',
                           'ang': 2.0,
                           'ang_deg': 2.0,
                           'energ': 2.0,
                           'T1': 57818.560277777775,
                           'T2': 57819.560277777775,
                           'T_format': 'mjd'},
                  )
    
    assert c.status_code == 200
    print("content:", c.text)
    jdata=c.json()
    # TODO notice the difference, is this acceptable?
    assert jdata['products']['analysis_parameters']['ang'] == 2
    assert jdata['products']['echo']['ang'] == 2
    # converted in the default units, which for the ang_deg parameter is arcsec
    assert jdata['products']['analysis_parameters']['ang_deg'] == 7200
    # in the products it instead remains in deg
    assert jdata['products']['echo']['ang_deg'] == 2

    assert jdata['products']['analysis_parameters']['energ'] == 2
    assert jdata['products']['echo']['energ'] == 2

    assert jdata['products']['analysis_parameters']['T1'] == '2017-03-06T13:26:48.000'
    assert jdata['products']['echo']['T1'] == 57818.560277777775
