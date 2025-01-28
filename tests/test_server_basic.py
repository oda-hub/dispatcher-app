import re
import shutil
import urllib
import io

import requests
import time
import uuid
import json
import os
import logging
import jwt
import glob
import pytest
import fcntl
from datetime import datetime, timedelta
from dateutil import parser, tz
from functools import reduce
from urllib import parse
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
import nbformat as nbf
import yaml
import gzip
import random
import string

from cdci_data_analysis.analysis.catalog import BasicCatalog
from cdci_data_analysis.pytest_fixtures import DispatcherJobState, ask, make_hash, dispatcher_fetch_dummy_products, make_hash_file
from cdci_data_analysis.flask_app.dispatcher_query import InstrumentQueryBackEnd
from cdci_data_analysis.analysis.renku_helper import clone_renku_repo, checkout_branch_renku_repo, check_job_id_branch_is_present, get_repo_path, generate_commit_request_url, create_new_notebook_with_code, generate_nb_hash, create_renku_ini_config_obj, generate_ini_file_hash
from cdci_data_analysis.analysis.drupal_helper import execute_drupal_request, get_drupal_request_headers, get_revnum, get_observations_for_time_range, generate_gallery_jwt_token, get_user_id, get_source_astrophysical_entity_id_by_source_name
from cdci_data_analysis.plugins.dummy_plugin.data_server_dispatcher import DataServerQuery, ReturnProgressProductQuery
from cdci_data_analysis.flask_app.app import sanitize_dict_before_log

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

specific_args = ['osa_version', 'E1_keV', 'E2_keV', 'max_pointings', 'radius']
def remove_args_from_dic(arg_dic, remove_keys):
    for key in remove_keys:
        arg_dic.pop(key, None)

default_exp_time = int(time.time()) + 5000
default_token_payload = dict(
    sub="mtm@mtmco.net",
    name="mmeharga",
    roles="general",
    exp=default_exp_time,
    tem=0,
)

@pytest.mark.fast
def test_sanitize_dict_before_log():

    test_dict = {
        'token': 'mytoken',
        'field': 'myfield\n\r',
        'username': 'myusername',
        'email': 'myemail@example.com'
    }

    expected_dict = {
        'field': 'myfield',
        'username': 'myusername',
        'email': 'myemailexamplecom'
    }

    sanitized_dict = sanitize_dict_before_log(test_dict)
    assert sanitized_dict == expected_dict

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
    assert c.status_code == 200
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
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

    print("test_empty_request content output:", c.text)

    jdata=c.json()

    assert c.status_code == 400

    # parameterize this
    assert sorted(jdata['installed_instruments']) == sorted(['empty', 'empty-async', 'empty-with-conf', 'empty-semi-async', 'empty-development', 'empty-async-return-progress', 'empty-with-posix-path']) or \
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


@pytest.mark.parametrize('fits_file_url', [ 'valid', 'invalid', 'empty'])
def test_load_frontend_fits_file_url(dispatcher_live_fixture, fits_file_url):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    # let's generate a valid token
    encoded_token = jwt.encode(default_token_payload, secret_key, algorithm='HS256')

    if fits_file_url == 'valid':
        fits_file_url = 'https://fits.gsfc.nasa.gov/samples/testkeys.fits'
        output_status_code = 200
    elif fits_file_url == 'invalid':
        fits_file_url = 'https://fits.gsfc.nasa.gov/samples/aaaaaa.fits'
        output_status_code = 404
    else:
        fits_file_url = None
        output_status_code = 400

    c=requests.get(os.path.join(server, 'load_frontend_fits_file_url'),
                   params={'fits_file_url': fits_file_url,
                           'token': encoded_token})

    assert c.status_code == output_status_code


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


def test_matrix_options_mode_empty_request(dispatcher_live_fixture_with_matrix_options):
    server = dispatcher_live_fixture_with_matrix_options
    print("constructed server:", server)

    c=requests.get(os.path.join(server, "run_analysis"),
                   params={},
                )

    print("content:", c.text)

    jdata=c.json()

    assert c.status_code == 400

    assert sorted(jdata['installed_instruments']) == sorted(
        ['empty', 'empty-async', 'empty-semi-async', 'empty-with-conf', 'empty-development', 'empty-async-return-progress', 'empty-with-posix-path',]) or \
           jdata['installed_instruments'] == []

    # assert jdata['debug_mode'] == "no"
    assert 'dispatcher-config' in jdata['config']

    dispatcher_config = jdata['config']['dispatcher-config']

    assert 'origin' in dispatcher_config

    assert 'sentry_url' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_port' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_host' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'secret_key' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'smtp_server_password' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'products_url' in dispatcher_config['cfg_dict']['dispatcher']

    assert 'matrix_sender_access_token' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'matrix_incident_report_sender_personal_access_token' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'matrix_bcc_receivers_room_ids' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'matrix_incident_report_receivers_room_ids' not in dispatcher_config['cfg_dict']['dispatcher']

    logger.info(jdata['config'])


@pytest.mark.not_safe_parallel
@pytest.mark.fast
def test_error_two_scratch_dir_same_job_id(dispatcher_live_fixture):
    DispatcherJobState.remove_scratch_folders()
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    encoded_token = jwt.encode(default_token_payload, secret_key, algorithm='HS256')
    # issuing a request each, with the same set of parameters
    params = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        token=encoded_token
    )
    DataServerQuery.set_status('submitted')
    # let's generate a fake scratch dir
    jdata = ask(server,
              params,
              expected_query_status=["submitted"],
              max_time_s=50,
              )

    job_id = jdata['job_monitor']['job_id']
    session_id = jdata['session_id']
    fake_scratch_dir = f'scratch_sid_01234567890_jid_{job_id}'
    os.makedirs(fake_scratch_dir)

    params['job_id'] = job_id
    params['session_id'] = session_id

    jdata = ask(server,
                params,
                expected_status_code=500,
                expected_query_status=None,
                )
    assert jdata['error'] == 'InternalError():We have encountered an internal error! Our team is notified and is working on it. We are sorry! When we find a solution we will try to reach you'
    assert jdata['error_message'] == 'We have encountered an internal error! Our team is notified and is working on it. We are sorry! When we find a solution we will try to reach you'
    os.rmdir(fake_scratch_dir)


@pytest.mark.not_safe_parallel
@pytest.mark.fast
def test_scratch_dir_creation_lock_error(dispatcher_live_fixture):
    DispatcherJobState.remove_scratch_folders()
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    encoded_token = jwt.encode(default_token_payload, secret_key, algorithm='HS256')
    # issuing a request each, with the same set of parameters
    params = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        token=encoded_token
    )
    DataServerQuery.set_status('submitted')
    # let's generate a fake scratch dir
    jdata = ask(server,
              params,
              expected_query_status=["submitted"],
              max_time_s=50,
              )

    job_id = jdata['job_monitor']['job_id']
    session_id = jdata['session_id']
    fake_scratch_dir = f'scratch_sid_01234567890_jid_{job_id}'
    os.makedirs(fake_scratch_dir)

    params['job_id'] = job_id
    params['session_id'] = session_id

    lock_file = f".lock_{job_id}"

    with open(lock_file, 'w') as f_lock:
        fcntl.flock(f_lock, fcntl.LOCK_EX)

        jdata = ask(server,
                    params,
                    expected_status_code=500,
                    expected_query_status=None,
                    )
    scratch_dir_retry_attempts = 5
    assert jdata['error'] == f"InternalError():Failed to acquire lock for directory creation after {scratch_dir_retry_attempts} attempts."
    assert jdata['error_message'] == f"Failed to acquire lock for directory creation after {scratch_dir_retry_attempts} attempts."
    os.rmdir(fake_scratch_dir)
    os.remove(lock_file)


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
def test_head_download_products_public(dispatcher_long_living_fixture, empty_products_files_fixture):
    server = dispatcher_long_living_fixture

    logger.info("constructed server: %s", server)

    session_id = empty_products_files_fixture['session_id']
    job_id = empty_products_files_fixture['job_id']

    params = {
        'query_status': 'ready',
        'file_list': 'test.fits.gz',
        'download_file_name': 'output_test',
        'session_id': session_id,
        'job_id': job_id
    }

    c = requests.head(server + "/download_products",
                     params=params)

    assert c.status_code == 200
    file_path = f'scratch_sid_{session_id}_jid_{job_id}/test.fits.gz'
    with open(file_path, "rb") as f_in:
        in_data = f_in.read()
    archived_file_path = f'scratch_sid_{session_id}_jid_{job_id}/output_test'
    with gzip.open(archived_file_path, 'wb') as f:
        f.write(in_data)
    # download the output, read it and then compare it
    size = os.path.getsize(archived_file_path)
    assert int(c.headers['Content-Length']) == size


@pytest.mark.fast
def test_download_products_aliased_dir(dispatcher_live_fixture):
    DispatcherJobState.remove_scratch_folders()
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
        'query_type': "Real",
        'instrument': 'empty-async',
        'p': 5.,
        'use_scws': 'user_file',
    }

    DataServerQuery.set_status('submitted')
    file_path = DispatcherJobState.create_p_value_file(p_value=5)
    list_file = open(file_path)

    jdata = ask(server,
                params,
                expected_query_status='submitted',
                expected_job_status=['submitted'],
                expected_status_code=200,
                max_time_s=150,
                method='post',
                files={'user_scw_list_file': list_file.read()}
                )
    list_file.close()

    list_file = open(file_path)
    DataServerQuery.set_status('done')
    # job done
    jdata_aliased = ask(server,
                        params,
                        expected_query_status='done',
                        expected_job_status=['done'],
                        expected_status_code=200,
                        max_time_s=150,
                        method='post',
                        files={'user_scw_list_file': list_file.read()}
                        )
    list_file.close()

    # force remove the file to test the download happens from the aliased dir
    os.remove(f'scratch_sid_{jdata["session_id"]}_jid_{jdata["job_monitor"]["job_id"]}/user_scw_list_file')

    d = requests.get(server + "/download_products",
                     params={
                         'session_id': jdata_aliased['session_id'],
                         'file_list': 'user_scw_list_file',
                         'download_file_name': 'output_test',
                         'query_status': 'ready',
                         'job_id': jdata_aliased['job_monitor']['job_id'],
                     })
    assert d.status_code == 200

@pytest.mark.fast
@pytest.mark.parametrize('filelist', ['../external_file', '/tmp/external_file', 'test.fits.gz'])
@pytest.mark.parametrize('outname', ['/tmp/output_test', '../output_test', 'output_test'])
def test_download_products_outside_dir(dispatcher_long_living_fixture, 
                                       empty_products_files_fixture,
                                       filelist,
                                       outname):    
    server = dispatcher_long_living_fixture

    is_good = True if filelist == 'test.fits.gz' and outname == 'output_test' else False
    logger.info("constructed server: %s", server)

    session_id = empty_products_files_fixture['session_id']
    job_id = empty_products_files_fixture['job_id']

    if not is_good:
        with open(filelist.replace('../', ''), 'w') as outb:
            outb.write('__confidential__')
    
    params = {
            'instrument': 'any_name',
            # since we are passing a job_id
            'query_status': 'ready',
            'file_list': filelist,
            'download_file_name': outname,
            'session_id': session_id,
            'job_id': job_id
        }

    c = requests.get(server + "/download_products",
                     params=params)

    if is_good:
        assert c.status_code == 200
        # further checks in previous test
    else:    
        assert c.status_code == 403
    
        # check the output anyway
        assert b"__confidential__" not in c.content
        if hasattr(c, 'text'):
            assert "__confidential__" not in c.text
        
        with io.BytesIO() as outb:
            outb.write(c.content)
            outb.seek(0)
            gz = gzip.GzipFile(fileobj=outb, mode='rb')
            with pytest.raises(gzip.BadGzipFile):
                gz.read()
        try:
            os.remove(filelist.replace('../', ''))
        except:
            pass


@pytest.mark.fast
@pytest.mark.parametrize("include_args", [True, False])
def test_download_file_redirection_external_products_url(dispatcher_live_fixture_with_external_products_url,
                                                           dispatcher_test_conf_with_external_products_url,
                                                           include_args):
    server = dispatcher_live_fixture_with_external_products_url

    logger.info("constructed server: %s", server)

    url_request = os.path.join(server, "download_file")

    if include_args:
        url_request += '?a=4566&token=aaaaaaaaaa'

    c = requests.get(url_request, allow_redirects=False)

    assert c.status_code == 302
    redirection_header_location_url = c.headers["Location"]
    redirection_url = os.path.join(dispatcher_test_conf_with_external_products_url['products_url'], 'dispatch-data/download_products')
    if include_args:
        redirection_url += '?a=4566&token=aaaaaaaaaa'
    redirection_url += '&from_request_files_dir=True&download_file=True&download_products=False'
    assert redirection_url == redirection_header_location_url


@pytest.mark.fast
@pytest.mark.parametrize("include_args", [True, False])
def test_download_file_redirection_default_route_products_url(dispatcher_live_fixture_with_default_route_products_url,
                                                              dispatcher_test_conf_with_default_route_products_url,
                                                              include_args):
    server = dispatcher_live_fixture_with_default_route_products_url

    logger.info("constructed server: %s", server)

    url_request = os.path.join(server, "download_file")

    if include_args:
        url_request += '?a=4566&token=aaaaaaaaaa'

    c = requests.get(url_request, allow_redirects=False)

    assert c.status_code == 302
    redirection_header_location_url = c.headers["Location"]
    redirection_url = os.path.join(dispatcher_test_conf_with_default_route_products_url['products_url'], 'dispatch-data/download_products')
    if include_args:
        redirection_url += '?a=4566&token=aaaaaaaaaa'
    redirection_url += '&from_request_files_dir=True&download_file=True&download_products=False'
    assert redirection_url == redirection_header_location_url


@pytest.mark.fast
@pytest.mark.parametrize("include_args", [True, False])
def test_download_file_redirection_no_custom_products_url(dispatcher_live_fixture_no_products_url,
                                                          include_args):
    server = dispatcher_live_fixture_no_products_url

    logger.info("constructed server: %s", server)

    url_request = os.path.join(server, "download_file")

    encoded_token = jwt.encode(default_token_payload, secret_key, algorithm='HS256')
    if include_args:
        url_request += '?a=4566&token=' + encoded_token

    c = requests.get(url_request, allow_redirects=False)

    assert c.status_code == 302
    redirection_header_location_url = c.headers["Location"]
    redirection_url = os.path.join(server, 'download_products')
    if include_args:
        redirection_url += '?a=4566&token=' + encoded_token
    redirection_url += '&from_request_files_dir=True&download_file=True&download_products=False'
    assert redirection_header_location_url == redirection_url


@pytest.mark.fast
@pytest.mark.parametrize('return_archive', [True, False])
@pytest.mark.parametrize('matching_file_name', [True, False])
def test_download_file_public(dispatcher_long_living_fixture, request_files_fixture, return_archive, matching_file_name):
    DispatcherJobState.create_local_request_files_folder()
    server = dispatcher_long_living_fixture

    logger.info("constructed server: %s", server)

    params = {
            'file_list': os.path.basename(request_files_fixture['file_path']),
            'download_file_name': 'output_test',
            'return_archive': return_archive,
        }

    if matching_file_name:
        params['download_file_name'] = params['file_list']

    c = requests.get(server + "/download_file",
                     params=params)

    assert c.status_code == 200

    # download the output, read it and then compare it
    with open('local_request_files/output_test', 'wb') as fout:
        fout.write(c.content)

    if return_archive:
        with gzip.open('local_request_files/output_test', 'rb') as fout:
            data_downloaded = fout.read()
    else:
        data_downloaded = c.content

    assert data_downloaded == request_files_fixture['content']


@pytest.mark.fast
@pytest.mark.parametrize('return_archive', [True, False])
@pytest.mark.parametrize('matching_file_name', [True, False])
def test_head_download_file(dispatcher_long_living_fixture, request_files_fixture, return_archive, matching_file_name):
    DispatcherJobState.create_local_request_files_folder()
    server = dispatcher_long_living_fixture

    logger.info("constructed server: %s", server)

    params = {
            'file_list': os.path.basename(request_files_fixture['file_path']),
            'download_file_name': 'output_test',
            'return_archive': return_archive,
        }

    if matching_file_name:
        params['download_file_name'] = params['file_list']

    c = requests.head(server + "/download_file",
                      allow_redirects=True,
                      params=params)

    assert c.status_code == 200

    if return_archive:
        with open(request_files_fixture['file_path'], "rb") as f_in:
            in_data = f_in.read()
        archived_file_path = f'local_request_files/{params["download_file_name"]}'
        with gzip.open(archived_file_path, 'wb') as f:
            f.write(in_data)
        # download the output, read it and then compare it
        size = os.path.getsize(archived_file_path)
    else:
        size = os.path.getsize(request_files_fixture['file_path'])

    assert int(c.headers['Content-Length']) == size


def test_query_restricted_instrument(dispatcher_live_fixture):
    server = dispatcher_live_fixture


    logger.info("constructed server: %s", server)

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty-development',
    }

    jdata = ask(server,
                params,
                expected_status_code=403,
                expected_query_status=None,
                max_time_s=150
                )

    assert jdata["debug_message"] == ""
    assert jdata["error_message"] == "Unfortunately, your priviledges are not sufficient to make the request for this instrument.\n"

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "roles": "oda workflow developer"
    }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty-development',
        'token': encoded_token
    }

    ask(server,
        params,
        expected_query_status=["done"],
        max_time_s=50
        )

    token_payload = {
        **default_token_payload,
        "roles": "general"
    }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty-development',
        'token': encoded_token
    }

    jdata = ask(server,
                params,
                expected_status_code=403,
                expected_query_status=None,
                max_time_s=150
                )

    assert jdata["debug_message"] == ""
    assert jdata["error_message"] == "Unfortunately, your priviledges are not sufficient to make the request for this instrument.\n"


@pytest.mark.fast
@pytest.mark.parametrize("include_args", [True, False])
def test_instrument_list_redirection_external_products_url(dispatcher_live_fixture_with_external_products_url,
                                                           dispatcher_test_conf_with_external_products_url,
                                                           include_args):
    server = dispatcher_live_fixture_with_external_products_url

    logger.info("constructed server: %s", server)

    url_request = os.path.join(server, "api/instr-list")

    if include_args:
        url_request += '?a=4566&token=aaaaaaaaaa'

    c = requests.get(url_request, allow_redirects=False)

    assert c.status_code == 302
    redirection_header_location_url = c.headers["Location"]
    redirection_url = os.path.join(dispatcher_test_conf_with_external_products_url['products_url'], 'dispatch-data/instr-list')
    if include_args:
        redirection_url += '?a=4566&token=aaaaaaaaaa'
    assert redirection_url == redirection_header_location_url


@pytest.mark.fast
@pytest.mark.parametrize("include_args", [True, False])
def test_instrument_list_redirection_default_route_products_url(dispatcher_live_fixture_with_default_route_products_url,
                                                                dispatcher_test_conf_with_default_route_products_url,
                                                                include_args):
    server = dispatcher_live_fixture_with_default_route_products_url

    logger.info("constructed server: %s", server)

    url_request = os.path.join(server, "api/instr-list")

    if include_args:
        url_request += '?a=4566&token=aaaaaaaaaa'

    c = requests.get(url_request, allow_redirects=False)

    assert c.status_code == 302
    redirection_header_location_url = c.headers["Location"]
    redirection_url = os.path.join(dispatcher_test_conf_with_default_route_products_url['products_url'], 'dispatch-data/instr-list')
    if include_args:
        redirection_url += '?a=4566&token=aaaaaaaaaa'
    assert redirection_url == redirection_header_location_url


@pytest.mark.fast
@pytest.mark.parametrize("allow_redirect", [True, False])
@pytest.mark.parametrize("include_args", [True, False])
def test_instrument_list_redirection_no_custom_products_url(dispatcher_live_fixture_no_products_url,
                                                        allow_redirect, include_args):
    server = dispatcher_live_fixture_no_products_url

    logger.info("constructed server: %s", server)

    url_request = os.path.join(server, "api/instr-list")

    encoded_token = jwt.encode(default_token_payload, secret_key, algorithm='HS256')
    if include_args:
        url_request += '?a=4566&token=' + encoded_token

    c = requests.get(url_request, allow_redirects=allow_redirect)

    if not allow_redirect:
        assert c.status_code == 302
        redirection_header_location_url = c.headers["Location"]
        redirection_url = os.path.join(server, 'instr-list')
        if include_args:
            redirection_url += '?a=4566&token=' + encoded_token
        assert redirection_header_location_url == redirection_url
    else:
        assert c.status_code == 200


@pytest.mark.fast
@pytest.mark.parametrize("allow_redirect", [True, False])
def test_instrument_list_redirection(dispatcher_live_fixture, allow_redirect):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    c = requests.get(os.path.join(server, "api/instr-list"), allow_redirects=allow_redirect)

    if not allow_redirect:
        assert c.status_code == 302
        redirection_header_location_url = c.headers["Location"]
        assert redirection_header_location_url == os.path.join(server, 'instr-list')
    else:
        assert c.status_code == 200


@pytest.mark.fast
@pytest.mark.parametrize("endpoint_url", ["instr-list", "api/instr-list"])
def test_per_user_instrument_list(dispatcher_live_fixture, endpoint_url):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    c = requests.get(os.path.join(server, endpoint_url))

    assert c.status_code == 200

    jdata = c.json()

    assert isinstance(jdata, list)
    assert not 'empty-development' in jdata

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "roles": "oda workflow developer"
    }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    c = requests.get(os.path.join(server, endpoint_url), params={"token": encoded_token})

    assert c.status_code == 200

    jdata = c.json()

    assert isinstance(jdata, list)
    assert 'empty-development' in jdata

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "roles": "general"
    }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    c = requests.get(os.path.join(server, endpoint_url), params={"token": encoded_token})

    assert c.status_code == 200

    jdata = c.json()

    assert isinstance(jdata, list)
    assert not 'empty-development' in jdata


@pytest.mark.fast
@pytest.mark.parametrize("endpoint_url", ["instr-list", "api/instr-list"])
def test_per_user_instrument_list_no_custom_products_url(dispatcher_live_fixture, endpoint_url):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    c = requests.get(os.path.join(server, endpoint_url))
        
    assert c.status_code == 200

    jdata = c.json()

    assert isinstance(jdata, list)
    assert not 'empty-development' in jdata

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "roles": "oda workflow developer"
    }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    c = requests.get(os.path.join(server, endpoint_url), params={"token": encoded_token})

    assert c.status_code == 200

    jdata = c.json()

    assert isinstance(jdata, list)
    assert 'empty-development' in jdata

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "roles": "general"
    }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    c = requests.get(os.path.join(server, endpoint_url), params={"token": encoded_token})

    assert c.status_code == 200

    jdata = c.json()

    assert isinstance(jdata, list)
    assert not 'empty-development' in jdata


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


@pytest.mark.parametrize("refresh_interval", [500000, 604800, 1000000, "exp_too_high"])
def test_refresh_token(dispatcher_live_fixture, dispatcher_test_conf, refresh_interval):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    # expired token
    token_payload = {
        **default_token_payload,
        "roles": "refresh-tokens"
    }

    if refresh_interval == "exp_too_high":
        token_payload["exp"] = 25340229700000
        refresh_interval_to_apply = 1
    else:
        refresh_interval_to_apply = refresh_interval

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        'token': encoded_token,
        'query_status': 'new',
        'refresh_interval': refresh_interval_to_apply
    }

    c = requests.post(server + "/refresh_token", params=params)

    if refresh_interval_to_apply > dispatcher_test_conf['token_max_refresh_interval']:
        jdata = c.json()
        assert jdata['error_message'] == 'Request not authorized'
        assert jdata['debug_message'] == 'The refresh interval requested exceeds the maximum allowed, please provide a value which is lower than 604800 seconds'
    else:
        token_update = {
            "exp": default_token_payload["exp"] + refresh_interval_to_apply
        }

        if refresh_interval == "exp_too_high":
            token_update["exp"] = 2177449199

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


@pytest.mark.parametrize("public_download_request", [True, False])
def test_arg_file(dispatcher_live_fixture, dispatcher_test_conf, public_download_request):
    DispatcherJobState.remove_scratch_folders()
    DispatcherJobState.empty_request_files_folders()
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
        'product_type': 'file_dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'p': 5.,
        'token': encoded_token,
    }

    p_file_path = DispatcherJobState.create_p_value_file(p_value=5)

    list_file = open(p_file_path)

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
                files={'dummy_file': list_file.read()}
                )

    list_file.close()
    assert 'dummy_file' in jdata['products']['analysis_parameters']
    parsed_url_dummy_file = parse.urlparse(jdata['products']['analysis_parameters']['dummy_file'])
    args_dict = parse.parse_qs(parsed_url_dummy_file.query)
    assert parsed_url_dummy_file.path.endswith('download_file')
    assert 'file_list' in args_dict
    assert len(args_dict['file_list']) == 1
    assert os.path.exists(f'request_files/{args_dict["file_list"][0]}')

    products_host_port = f"http://{dispatcher_test_conf['bind_options']['bind_host']}:{dispatcher_test_conf['bind_options']['bind_port']}"

    arg_download_url = jdata['products']['analysis_parameters']['dummy_file'].replace('PRODUCTS_URL/', products_host_port)

    file_hash = make_hash_file(p_file_path)
    dpars = urlencode(dict(file_list=file_hash,
                           _is_mmoda_url=True,
                           return_archive=False))
    local_download_url = f"{os.path.join(products_host_port, 'download_file')}?{dpars}"

    assert arg_download_url == local_download_url

    if public_download_request:
        c = requests.get(arg_download_url)
        assert c.status_code == 403
        jdata = c.json()
        assert jdata['exit_status']['message'] == "User cannot access the file"
    else:
        arg_download_url += f'&token={encoded_token}'
        c = requests.get(arg_download_url)
        assert c.status_code == 200
        with open(p_file_path) as p_file:
            p_file_content = p_file.read()
        assert c.content.decode() == p_file_content

@pytest.mark.parametrize("public_download_request", [True, False])
def test_arg_file_external_product_url(dispatcher_live_fixture_with_external_products_url,
                                       dispatcher_test_conf_with_external_products_url,
                                       public_download_request):
    DispatcherJobState.remove_scratch_folders()
    DispatcherJobState.empty_request_files_folders()
    server = dispatcher_live_fixture_with_external_products_url
    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "unige-hpc-full, general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': 'file_dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'p': 5.,
        'token': encoded_token,
    }

    p_file_path = DispatcherJobState.create_p_value_file(p_value=5)

    list_file = open(p_file_path)

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
                files={'dummy_file': list_file.read()}
                )

    list_file.close()
    assert 'dummy_file' in jdata['products']['analysis_parameters']
    parsed_url_dummy_file = parse.urlparse(jdata['products']['analysis_parameters']['dummy_file'])
    args_dict = parse.parse_qs(parsed_url_dummy_file.query)
    assert parsed_url_dummy_file.path.endswith('download_file')
    assert 'file_list' in args_dict
    assert len(args_dict['file_list']) == 1
    assert os.path.exists(f'request_files/{args_dict["file_list"][0]}')

    file_hash = make_hash_file(p_file_path)
    dpars = urlencode(dict(file_list=file_hash,
                           _is_mmoda_url=True,
                           return_archive=False))
    local_download_url = f"{os.path.join(dispatcher_test_conf_with_external_products_url['products_url'], 'dispatch-data/download_file')}?{dpars}"

    assert jdata['products']['analysis_parameters']['dummy_file'] == local_download_url

@pytest.mark.parametrize("public_download_request", [True, False])
def test_arg_file_default_product_url(dispatcher_live_fixture_with_default_route_products_url,
                                      dispatcher_test_conf_with_default_route_products_url,
                                      public_download_request):
    DispatcherJobState.remove_scratch_folders()
    DispatcherJobState.empty_request_files_folders()
    server = dispatcher_live_fixture_with_default_route_products_url
    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "unige-hpc-full, general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'product_type': 'file_dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'p': 5.,
        'token': encoded_token,
    }

    p_file_path = DispatcherJobState.create_p_value_file(p_value=5)

    list_file = open(p_file_path)

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
                files={'dummy_file': list_file.read()}
                )

    list_file.close()
    assert 'dummy_file' in jdata['products']['analysis_parameters']
    parsed_url_dummy_file = parse.urlparse(jdata['products']['analysis_parameters']['dummy_file'])
    args_dict = parse.parse_qs(parsed_url_dummy_file.query)
    assert parsed_url_dummy_file.path.endswith('download_file')
    assert 'file_list' in args_dict
    assert len(args_dict['file_list']) == 1
    assert os.path.exists(f'request_files/{args_dict["file_list"][0]}')

    file_hash = make_hash_file(p_file_path)
    dpars = urlencode(dict(file_list=file_hash,
                           _is_mmoda_url=True,
                           return_archive=False))
    local_download_url = f"{os.path.join(dispatcher_test_conf_with_default_route_products_url['products_url'], 'dispatch-data/download_file')}?{dpars}"

    assert jdata['products']['analysis_parameters']['dummy_file'] == local_download_url

def test_file_ownerships(dispatcher_live_fixture):
    DispatcherJobState.remove_scratch_folders()
    DispatcherJobState.empty_request_files_folders()
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
        'product_type': 'file_dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'p': 5.,
        'token': encoded_token,
    }

    p_file_path_first = DispatcherJobState.create_p_value_file(p_value=5)
    p_file_path_second = DispatcherJobState.create_p_value_file(p_value=6)

    list_file_first = open(p_file_path_first)
    list_file_second = open(p_file_path_second)

    expected_query_status = 'done'
    expected_job_status = 'done'
    expected_status_code = 200

    ask(server,
        params,
        expected_query_status=expected_query_status,
        expected_job_status=expected_job_status,
        expected_status_code=expected_status_code,
        max_time_s=150,
        method='post',
        files={'dummy_file_first': list_file_first.read(), 'dummy_file_second': list_file_second.read()}
        )

    list_file_first.close()
    list_file_second.close()

    first_file_hash = make_hash_file(p_file_path_first)
    second_file_hash = make_hash_file(p_file_path_second)

    first_ownership_file_path = os.path.join('request_files', first_file_hash + '_ownerships.json')
    second_ownership_file_path = os.path.join('request_files', second_file_hash + '_ownerships.json')
    assert os.path.exists(first_ownership_file_path)
    assert os.path.exists(second_ownership_file_path)

    with open(first_ownership_file_path) as first_ownership_file:
        first_ownerships = json.load(first_ownership_file)
    with open(second_ownership_file_path) as second_ownership_file:
        second_ownerships = json.load(second_ownership_file)

    assert token_payload['sub'] in first_ownerships['user_emails']
    assert token_payload['sub'] in second_ownerships['user_emails']
    token_roles = [r.strip() for r in token_payload['roles'].split(',')]
    assert all(r in first_ownerships['user_roles'] for r in token_roles)
    assert all(r in second_ownerships['user_roles'] for r in token_roles)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "sub":"mtm2@mtmco.net",
        "name":"mmeharga2",
        "roles": "general, unige-second-hpc-full, general_second_request",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    params['token'] = encoded_token

    list_file_first = open(p_file_path_first)
    ask(server,
        params,
        expected_query_status=expected_query_status,
        expected_job_status=expected_job_status,
        expected_status_code=expected_status_code,
        max_time_s=150,
        method='post',
        files={'dummy_file_first': list_file_first.read()}
        )

    list_file_first.close()

    with open(first_ownership_file_path) as first_ownership_file:
        first_ownerships = json.load(first_ownership_file)

    assert token_payload['sub'] in first_ownerships['user_emails']
    token_roles = [r.strip() for r in token_payload['roles'].split(',')]
    assert all(r in first_ownerships['user_roles'] for r in token_roles)


def test_public_file_ownerships(dispatcher_live_fixture):
    DispatcherJobState.remove_scratch_folders()
    DispatcherJobState.empty_request_files_folders()
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'p': 6.,
    }

    p_file_path = DispatcherJobState.create_p_value_file(p_value=6)
    list_file = open(p_file_path)

    expected_query_status = 'done'
    expected_job_status = 'done'
    expected_status_code = 200

    ask(server,
        params,
        expected_query_status=expected_query_status,
        expected_job_status=expected_job_status,
        expected_status_code=expected_status_code,
        max_time_s=150,
        method='post',
        files={'dummy_file_first': list_file.read()}
        )

    list_file.close()
    file_hash = make_hash_file(p_file_path)

    ownership_file_path = os.path.join('request_files', file_hash + '_ownerships.json')
    assert os.path.exists(ownership_file_path)
    with open(ownership_file_path) as ownership_file:
        ownerships = json.load(ownership_file)
    assert ownerships['user_roles'] == []


@pytest.mark.parametrize("include_file_arg", [True, False])
def test_default_value_empty_posix_path(dispatcher_live_fixture, include_file_arg):
    DispatcherJobState.remove_scratch_folders()
    DispatcherJobState.empty_request_files_folders()
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
        'product_type': 'file_dummy',
        'query_type': "Dummy",
        'instrument': 'empty-with-posix-path',
        'dummy_POSIX_file_type': 'file',
        'token': encoded_token
    }

    p_file_path = DispatcherJobState.create_p_value_file(p_value=6)
    list_file = open(p_file_path)

    expected_query_status = 'done'
    expected_job_status = 'done'
    expected_status_code = 200

    files = None
    if include_file_arg:
        files = {'dummy_POSIX_file': list_file.read()}

    jdata = ask(server,
                params,
                expected_query_status=expected_query_status,
                expected_job_status=expected_job_status,
                expected_status_code=expected_status_code,
                max_time_s=150,
                method='post',
                files=files
                )

    list_file.close()
    assert 'dummy_POSIX_file' in jdata['products']['analysis_parameters']
    if include_file_arg:
        assert jdata['products']['analysis_parameters']['dummy_POSIX_file'] is not None
    else:
        assert jdata['products']['analysis_parameters']['dummy_POSIX_file'] is None


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
        'token': encoded_token,
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
    remove_args_from_dic(restricted_par_dic, specific_args)
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
        remove_args_from_dic(restricted_par_dic, specific_args)
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
        'token': encoded_token,
        'allow_unknown_args': True
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


@pytest.mark.parametrize("query_type", ["Dummy", "Real"])
def test_empty_async_return_progress_instrument_request(dispatcher_live_fixture, query_type):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    ReturnProgressProductQuery.set_p_value(5)

    params = {
        **default_params,
        'product_type': 'dummy',
        'query_type': query_type,
        'instrument': 'empty-async-return-progress',
        'return_progress': True
    }

    jdata = ask(server,
                params,
                expected_query_status=["submitted"],
                max_time_s=50,
                )

    logger.info("Json output content")
    logger.info(json.dumps(jdata, indent=4))

    assert jdata["exit_status"]["debug_message"] == ""
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == ""

    assert jdata["products"]["p"] == 5

    params.pop("return_progress", None)
    ReturnProgressProductQuery.set_p_value(15)

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

    assert jdata["products"]["p"] == 15


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
    example_config.pop('matrix_options', None)

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
    example_config.pop('matrix_options', None)

    mapper = lambda x, y: ".".join(map(str, x))
    example_config_keys = flatten_nested_structure(example_config, mapper)
    test_config_keys = flatten_nested_structure(dispatcher_test_conf_with_gallery, mapper)

    print("\n\n\nexample_config_keys", example_config_keys)
    print("\n\n\ntest_config_keys", test_config_keys)

    assert set(example_config_keys) == set(test_config_keys)


def test_example_config_with_matrix_options(dispatcher_test_conf_with_matrix_options):
    import cdci_data_analysis.config_dir

    example_config_fn = os.path.join(
        os.path.dirname(cdci_data_analysis.__file__),
        "config_dir/conf_env.yml.example"
    )
    with open(example_config_fn) as example_config_fn_f:
        example_config = yaml.load(example_config_fn_f, Loader=yaml.SafeLoader)['dispatcher']
    example_config.pop('product_gallery_options', None)

    mapper = lambda x, y: ".".join(map(str, x))
    example_config_keys = flatten_nested_structure(example_config, mapper)
    test_config_keys = flatten_nested_structure(dispatcher_test_conf_with_matrix_options, mapper)

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
    remove_args_from_dic(restricted_par_dic, specific_args)
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
        params['allow_unknown_args'] = True

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
    remove_args_from_dic(restricted_par_dic, specific_args)
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
@pytest.mark.parametrize("request_type", ["private", "public"])
def test_source_resolver(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, source_to_resolve, request_type):
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

    if request_type == "private":
        params.pop('token', None)

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
        assert resolved_obj['message'].startswith(f'{source_to_resolve} could not be resolved')
    else:
        assert 'name' in resolved_obj
        assert 'DEC' in resolved_obj
        assert 'RA' in resolved_obj
        assert 'entity_portal_link' in resolved_obj
        assert 'object_ids' in resolved_obj
        assert 'object_type' in resolved_obj
        assert 'message' in resolved_obj

        assert resolved_obj['name'] == source_to_resolve.replace('_', ' ')
        assert resolved_obj['entity_portal_link'] == dispatcher_test_conf_with_gallery["product_gallery_options"]["entities_portal_url"]\
            .format(urllib.parse.quote(source_to_resolve.strip()))


@pytest.mark.test_drupal
@pytest.mark.parametrize("source_to_resolve", ['Mrk 421', 'Mrk_421', 'GX 1+4', 'fake object', None])
def test_source_resolver_invalid_local_resolver(dispatcher_live_fixture_with_gallery_invalid_local_resolver, dispatcher_test_conf_with_gallery_invalid_local_resolver, source_to_resolve):
    server = dispatcher_live_fixture_with_gallery_invalid_local_resolver

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
        assert resolved_obj['message'].startswith(f'{source_to_resolve} could not be resolved')
    else:
        assert 'name' in resolved_obj
        assert 'DEC' in resolved_obj
        assert 'RA' in resolved_obj
        assert 'entity_portal_link' in resolved_obj
        assert 'object_ids' in resolved_obj
        assert 'object_type' in resolved_obj
        assert 'message' in resolved_obj

        assert resolved_obj['name'] == source_to_resolve.replace('_', ' ')
        assert resolved_obj['entity_portal_link'] == dispatcher_test_conf_with_gallery_invalid_local_resolver["product_gallery_options"]["entities_portal_url"]\
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
@pytest.mark.parametrize("timerange_parameters", ["time_range_no_timezone_same_time", "time_range_no_timezone", "time_range_no_timezone_limits", "time_range_with_timezone", "new_time_range", "observation_id"])
def test_product_gallery_data_product_with_period_of_observation(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, timerange_parameters):
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
        'obsid': '1960001,1960002,1960003'
    }

    file_obj = {'yaml_file_0': open('observation_yaml_dummy_files/obs_rev_2542.yaml', 'rb')}

    now = datetime.now()

    if timerange_parameters == 'time_range_no_timezone':
        params['T1'] = '2022-07-21T00:29:47'
        params['T2'] = '2022-07-23T05:29:11'
    elif timerange_parameters == 'time_range_no_timezone_same_time':
        params['T1'] = '2023-02-10T01:17:00'
        params['T2'] = '2023-02-10T01:17:00'
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
                      data=params,
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

    link_field_field_attachments = os.path.join(
        dispatcher_test_conf_with_gallery['product_gallery_options']['product_gallery_url'],
        'rest/relation/node/observation/field_attachments')
    assert link_field_field_attachments in drupal_res_obs_info_obj['_links']

    obs_per_field_timerange_start_no_timezone = parser.parse(obs_per_field_timerange[0]['value']).strftime('%Y-%m-%dT%H:%M:%S')
    obs_per_field_timerange_end_no_timezone = parser.parse(obs_per_field_timerange[0]['end_value']).strftime(
        '%Y-%m-%dT%H:%M:%S')

    if timerange_parameters in ['time_range_no_timezone', 'time_range_with_timezone', 'new_time_range', 'time_range_no_timezone_limits', 'time_range_no_timezone_same_time']:
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
@pytest.mark.parametrize("update_astro_entity", [True, False])
def test_product_gallery_update_new_astrophysical_entity(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, update_astro_entity):
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
        'update_astro_entity': update_astro_entity
    }

    c = requests.post(os.path.join(server, "post_astro_entity_to_gallery"),
                      params={**params},
                      )

    drupal_res_obj = c.json()
    if update_astro_entity:
        assert c.status_code == 400
        assert 'drupal_helper_error_message' in drupal_res_obj
        assert 'error while updating astrophysical and entity product: no correspondent entity could be found with the provided name' \
               in drupal_res_obj['drupal_helper_error_message']
    else:
        assert c.status_code == 200
        assert drupal_res_obj['title'][0]['value'] == params['src_name']


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
def test_product_gallery_get_all_revs(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery):
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
        'title': "rev. test",
        'T1': (now - timedelta(days=random.randint(30, 150))).strftime('%Y-%m-%dT%H:%M:%S'),
        'T2': now.strftime('%Y-%m-%dT%H:%M:%S')
    }

    c = requests.post(os.path.join(server, "post_observation_to_gallery"),
                      params={**params},
                      )

    assert c.status_code == 200

    c = requests.get(os.path.join(server, "get_all_revs"),
                     params={'token': encoded_token}
                     )

    assert c.status_code == 200
    drupal_res_obj = c.json()

    assert isinstance(drupal_res_obj, list)
    assert params['title'] in drupal_res_obj


@pytest.mark.test_drupal
def test_product_gallery_get_all_astro_entities(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery):
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
        'src_name': 'test astro entity' + '_' + str(uuid.uuid4())
    }

    c = requests.post(os.path.join(server, "post_astro_entity_to_gallery"),
                      params={**params},
                      )

    assert c.status_code == 200

    c = requests.get(os.path.join(server, "get_all_astro_entities"),
                     params={'token': encoded_token}
                     )

    assert c.status_code == 200
    drupal_res_obj = c.json()

    assert isinstance(drupal_res_obj, list)
    assert any(src['title'] == params['src_name'] for src in drupal_res_obj)


@pytest.mark.test_drupal
@pytest.mark.parametrize("source_name", ["new", "known", "unknown"])
@pytest.mark.parametrize("include_products_fields_conditions", [True, False])
@pytest.mark.parametrize("request_type", ["private", "public"])
def test_product_gallery_get_data_products_list_with_conditions(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, source_name, include_products_fields_conditions, request_type):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    instrument_name = 'isgri'
    product_type = 'isgri_image'
    instrument_query = 'isgri'
    product_type_query = 'image'

    if source_name == 'new':
        source_name = 'test astro entity' + '_' + str(uuid.uuid4())
        # let's create a source
        source_params = {
            'token': encoded_token,
            'src_name': source_name
        }

        c = requests.post(os.path.join(server, "post_astro_entity_to_gallery"),
                          data=source_params,
                          )

        assert c.status_code == 200

        # let's post a product with the source just created
        product_params = {
            'instrument': instrument_name,
            'product_type': product_type,
            'E1_keV': 150,
            'E2_keV': 350,
            'src_name': source_name,
            'content_type': 'data_product',
            'token': encoded_token,
            'insert_new_source': True,
            'T1': '2022-07-21T00:29:47',
            'T2': '2022-08-23T05:29:11'
        }
        c = requests.post(os.path.join(server, "post_product_to_gallery"),
                          data=product_params
                          )

        assert c.status_code == 200

        params = {
            'token': encoded_token,
            'src_name': source_name,
            'instrument_name': instrument_query,
            'product_type': product_type_query
        }

        if request_type == "public":
            params.pop('token')

        if include_products_fields_conditions:
            for e1_kev, e2_kev, rev1, rev2 in [
                (100, 350, 2528, 2540),
                (100, 350, 2526, 2541),
                (100, 350, 2529, 2539),
                (100, 350, 2529, 2541),
                (100, 350, 2527, 2539),
                (50, 400, 2528, 2540),
                (200, 350, 2528, 2540),
                (200, 350, 2528, 2540),
                (50, 300, 2528, 2540),
            ]:
                logger.info(f"testing with e1_kev_value {e1_kev}, e2_kev_value {e2_kev}")
                params['e1_kev_value'] = e1_kev
                params['e2_kev_value'] = e2_kev

                params['rev1_value'] = rev1
                params['rev2_value'] = rev2

                c = requests.get(os.path.join(server, "get_data_product_list_with_conditions"),
                                 params=params
                                 )

                assert c.status_code == 200
                drupal_res_obj = c.json()
                assert isinstance(drupal_res_obj, list)

                if e1_kev > 100 or e2_kev < 350 or rev1 > 2528 or rev2 < 2540:
                    assert len(drupal_res_obj) == 0
                else:
                    assert len(drupal_res_obj) == 1
        else:
            c = requests.get(os.path.join(server, "get_data_product_list_with_conditions"),
                             params=params
                             )

            assert c.status_code == 200
            drupal_res_obj = c.json()
            assert isinstance(drupal_res_obj, list)
            assert len(drupal_res_obj) == 1
    elif source_name == 'unknown':
        source_name = "aaaaaaaaaaaaaaaaa"
        params = {
            'token': encoded_token,
            'src_name': source_name
        }
        c = requests.get(os.path.join(server, "get_data_product_list_with_conditions"),
                         params=params
                         )

        assert c.status_code == 200
        drupal_res_obj = c.json()
        assert isinstance(drupal_res_obj, list)
        assert len(drupal_res_obj) == 0
    else:
        source_name = "V404 Cyg"
        params = {
            'token': encoded_token,
            'src_name': source_name
        }
        c = requests.get(os.path.join(server, "get_data_product_list_with_conditions"),
                         params=params
                         )

        assert c.status_code == 200
        drupal_res_obj_source_name = c.json()
        assert isinstance(drupal_res_obj_source_name, list)

        source_name = "1RXS J202405.3+335157"
        params = {
            'token': encoded_token,
            'src_name': source_name
        }
        c = requests.get(os.path.join(server, "get_data_product_list_with_conditions"),
                         params=params
                         )

        assert c.status_code == 200
        drupal_res_obj_alternative_name = c.json()
        assert isinstance(drupal_res_obj_alternative_name, list)

        assert len(drupal_res_obj_alternative_name) == len(drupal_res_obj_source_name)

        # Create sets of dictionaries
        set1 = set(map(lambda d: frozenset(d.items()), drupal_res_obj_source_name))
        set2 = set(map(lambda d: frozenset(d.items()), drupal_res_obj_alternative_name))

        # Find the differences
        diff1 = set1 - set2
        diff2 = set2 - set1

        assert diff2 == set()
        assert diff1 == set()


@pytest.mark.test_drupal
@pytest.mark.parametrize("source_name", ["new", "known"])
def test_product_gallery_get_data_products_list_for_given_source(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, source_name):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    if source_name == 'new':
        source_name = 'test astro entity' + '_' + str(uuid.uuid4())
        # let's create a source
        source_params = {
            'token': encoded_token,
            'src_name': source_name
        }

        c = requests.post(os.path.join(server, "post_astro_entity_to_gallery"),
                          data=source_params,
                          )

        assert c.status_code == 200

        # let's post a product with the source just created
        product_params = {
            'instrument': 'empty',
            'ra': 150,
            'dec': 350,
            'src_name': source_name,
            'content_type': 'data_product',
            'token': encoded_token,
            'insert_new_source': True
        }
        c = requests.post(os.path.join(server, "post_product_to_gallery"),
                          data=product_params
                          )

        assert c.status_code == 200

        params = {
            'token': encoded_token,
            'src_name': source_name
        }
        c = requests.get(os.path.join(server, "get_data_product_list_by_source_name"),
                         params=params
                         )

        assert c.status_code == 200
        drupal_res_obj = c.json()

        assert isinstance(drupal_res_obj, list)

        assert len(drupal_res_obj) == 1
        assert 'ra' in drupal_res_obj[0]
        assert float(drupal_res_obj[0]['ra']) == float(product_params['ra'])
        assert 'dec' in drupal_res_obj[0]
        assert float(drupal_res_obj[0]['dec']) == float(product_params['dec'])
    else:
        source_name = "V404 Cyg"
        params = {
            'token': encoded_token,
            'src_name': source_name
        }
        c = requests.get(os.path.join(server, "get_data_product_list_by_source_name"),
                         params=params
                         )

        assert c.status_code == 200
        drupal_res_obj_source_name = c.json()
        assert isinstance(drupal_res_obj_source_name, list)

        source_name = "1RXS J202405.3+335157"
        params = {
            'token': encoded_token,
            'src_name': source_name
        }
        c = requests.get(os.path.join(server, "get_data_product_list_by_source_name"),
                         params=params
                         )

        assert c.status_code == 200
        drupal_res_obj_alternative_name = c.json()
        assert isinstance(drupal_res_obj_alternative_name, list)

        assert len(drupal_res_obj_alternative_name) == len(drupal_res_obj_source_name)

        # Create sets of dictionaries
        set1 = set(map(lambda d: frozenset(d.items()), drupal_res_obj_source_name))
        set2 = set(map(lambda d: frozenset(d.items()), drupal_res_obj_alternative_name))

        # Find the differences
        diff1 = set1 - set2
        diff2 = set2 - set1

        assert diff2 == set()
        assert diff1 == set()


@pytest.mark.test_drupal
@pytest.mark.parametrize("source_name", ["known", "unknown"])
@pytest.mark.parametrize("anonymous", ["known", "unknown"])
def test_product_gallery_astro_entity_info(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery, source_name, anonymous):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    if source_name == 'unknown':
        source_name = 'test astro entity' + '_' + str(uuid.uuid4())

        params = {
            'src_name': source_name
        }
        if not anonymous:
            params['token'] = encoded_token

        c = requests.get(os.path.join(server, "get_astro_entity_info_by_source_name"),
                         params=params
                         )

        assert c.status_code == 200
        drupal_res_obj = c.json()

        assert drupal_res_obj == {}

    else:
        source_name = "V404 Cyg"
        params = {
            'src_name': source_name
        }

        if not anonymous:
            params['token'] = encoded_token

        c = requests.get(os.path.join(server, "get_astro_entity_info_by_source_name"),
                         params=params
                         )

        assert c.status_code == 200
        drupal_res_obj_source_name = c.json()
        assert 'source_ra' in drupal_res_obj_source_name
        assert 'source_dec' in drupal_res_obj_source_name
        assert 'alternative_names_long_str' in drupal_res_obj_source_name
        assert 'title' in drupal_res_obj_source_name
        assert 'url' in drupal_res_obj_source_name
        assert 'url_preview' in drupal_res_obj_source_name
        assert 'nid' in drupal_res_obj_source_name


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

    with open('observation_yaml_dummy_files/obs_rev_2542.yaml', 'rb') as yaml_test_fn:
        file_obj = {'yaml_file_0': yaml_test_fn}


        c = requests.post(os.path.join(server, "post_observation_to_gallery"),
                          data=params,
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

    assert yaml_file_content_obs_rev_2542 in drupal_res_obj['file_content']


@pytest.mark.test_drupal
def test_product_gallery_get_not_existing_period_of_observation_attachments(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery):
    server = dispatcher_live_fixture_with_gallery

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, gallery contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    c = requests.get(os.path.join(server, "get_observation_attachments"),
                     params={'title': 'rev. aaaaa',
                             'token': encoded_token}
                     )

    assert c.status_code == 200
    assert c.json() == {}


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
                      data=params,
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
def test_revolution_processing_log_gallery_post(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery):
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
    params = {
        'content_type': 'revolution_processing_log',
        'revolution_number': 1,
        'sbatch_job_id': "155111",
        'log': 'test log',
        'type': 'success',
        'token': encoded_token
    }

    c = requests.post(os.path.join(server, "post_revolution_processing_log_to_gallery"),
                      data=params,
                      )

    assert c.status_code == 200

    drupal_res_obj = c.json()

    assert 'title' in drupal_res_obj

    assert 'field_log' in drupal_res_obj
    assert drupal_res_obj['field_log'][0]['value'] == params['log']

    assert 'field_sbatch_job_id' in drupal_res_obj
    assert drupal_res_obj['field_sbatch_job_id'][0]['value'] == params['sbatch_job_id']

    assert 'field_revolution_number' in drupal_res_obj
    assert drupal_res_obj['field_revolution_number'][0]['value'] == params['revolution_number']

    assert 'field_type' in drupal_res_obj
    assert drupal_res_obj['field_type'][0]['value'] == params['type']


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
                      data=params,
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
                      data=params
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
                      data=params,
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
                      data=params,
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
def test_product_gallery_delete(dispatcher_live_fixture_with_gallery, dispatcher_test_conf_with_gallery):
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
    product_id = 'aaabbbccc_' + str(time.time())

    params = dict(product_id=product_id,
                  instrument='isgri',
                  src_name='Crab',
                  product_type='isgri_lc',
                  content_type='data_product',
                  E1_keV=45,
                  E2_kev=95,
                  DEC=15,
                  RA=458,
                  product_title='Test data product to be deleted',
                  token=encoded_token)

    c = requests.post(os.path.join(server, "post_product_to_gallery"),
                      data=params,
                      )

    assert c.status_code == 200

    drupal_res_obj = c.json()

    assert 'nid' in drupal_res_obj
    nid_creation = drupal_res_obj['nid'][0]['value']
    assert 'field_product_id' in drupal_res_obj
    assert drupal_res_obj['field_product_id'][0]['value'] == product_id

    params_products_list = {
        'product_id_value': product_id,
        'content_type': 'data_product',
        'token': encoded_token
    }

    c = requests.get(os.path.join(server, "get_data_product_list_with_conditions"),
                     params=params_products_list
                     )

    assert c.status_code == 200
    drupal_res_obj = c.json()
    assert len(drupal_res_obj) == 1
    assert drupal_res_obj[0]['nid'] == str(nid_creation)

    params = {
        'product_id': product_id,
        'content_type': 'data_product',
        'token': encoded_token
    }

    c = requests.post(os.path.join(server, "delete_product_to_gallery"),
                      data=params,
                      )
    assert c.status_code == 200

    drupal_res_obj = c.json()
    assert drupal_res_obj == {}

    c = requests.get(os.path.join(server, "get_data_product_list_with_conditions"),
                     params=params_products_list
                     )

    assert c.status_code == 200
    drupal_res_obj = c.json()
    assert drupal_res_obj == []

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
                      data=params,
                      )

    assert c.status_code == 500

    drupal_res_obj = c.json()

    assert 'drupal_helper_error_message' in drupal_res_obj
    assert 'InvalidArgumentException: Field field_e3_kev is unknown.' \
           in drupal_res_obj['drupal_helper_error_message']


@pytest.mark.test_renku
def test_posting_renku_error_missing_file(dispatcher_live_fixture_with_renku_options, dispatcher_test_conf_with_renku_options):
    DispatcherJobState.remove_scratch_folders()
    server = dispatcher_live_fixture_with_renku_options
    print("constructed server:", server)
    logger.info("constructed server: %s", server)

    token_payload = {
        **default_token_payload,
        "roles": "general, renku contributor",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    p = 7.5

    params = {
        **default_params,
        'src_name': 'Mrk 421',
        'product_type': 'numerical',
        'query_type': "Dummy",
        'instrument': 'empty',
        'p': p,
        'token': encoded_token
    }

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=150
                )
    job_id = jdata['products']['job_id']
    session_id = jdata['products']['session_id']
    scratch_dir_fn = f'scratch_sid_{session_id}_jid_{job_id}'
    os.remove(os.path.join(scratch_dir_fn, "analysis_parameters.json"))

    params = {
        'job_id': job_id,
        'token': encoded_token
    }

    c = requests.post(os.path.join(server, "push-renku-branch"),
                      params={**params}
                      )

    assert c.status_code == 400

    jdata = c.json()
    assert jdata['error_message'] == ('Internal error while posting on the renku branch. '
                                      'Our team is notified and is working on it.')


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
    p = 7.5

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

    api_code_file_name = 'api_code.ipynb'
    api_code_file_path = os.path.join(repo.working_dir, api_code_file_name)

    extracted_api_code = DispatcherJobState.extract_api_code(session_id, job_id)
    token_pattern = r"[\'\"]token[\'\"]:\s*?[\'\"].*?[\'\"]"
    extracted_api_code = "import os\n\n" + re.sub(token_pattern, '"token": os.environ[\'ODA_TOKEN\'],', extracted_api_code, flags=re.DOTALL)

    nb_obj = create_new_notebook_with_code(extracted_api_code)
    notebook_hash = generate_nb_hash(nb_obj)

    config_ini_obj = create_renku_ini_config_obj(repo, api_code_file_name)
    config_ini_hash = generate_ini_file_hash(config_ini_obj)

    repo = checkout_branch_renku_repo(repo, branch_name=f'mmoda_request_{job_id}_{notebook_hash}_{config_ini_hash}', pull=True)

    assert check_job_id_branch_is_present(repo, job_id, notebook_hash, config_ini_hash)

    assert c.text == f"{renku_project_url}/sessions/new?autostart=1&branch=mmoda_request_{job_id}_{notebook_hash}_{config_ini_hash}" \
                     f"&commit={repo.head.commit.hexsha}" \
                     f"&env[ODA_TOKEN]={encoded_token}"
                     # f"&notebook={api_code_file_name}" \

    logger.info("Renku url: %s", c.text)

    assert os.path.exists(api_code_file_path)
    parsed_notebook = nbf.read(api_code_file_path, 4)
    assert len(parsed_notebook.cells) == 2
    assert parsed_notebook.cells[0].source == "# Notebook automatically generated from MMODA"
    assert parsed_notebook.cells[1].source == extracted_api_code

    request_url = generate_commit_request_url(products_url, request_dict)
    notebook_commit_message = (f"Stored API code of MMODA request by {token_payload['name']} for a {request_dict['product_type']}"
                      f" from the instrument {request_dict['instrument']}"
                      f"\nthe original request was generated via {request_url}\n"
                      "to retrieve the result please follow the link")

    git_notebook_commit_msg = list(repo.iter_commits(paths=api_code_file_name))[0].message
    config_file_commit_msg = list(repo.iter_commits(paths='.renku/renku.ini'))[0].message

    assert git_notebook_commit_msg == notebook_commit_message
    assert config_file_commit_msg == 'Update Renku config file with starting notebook'

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
                           'T_format': 'mjd',
                           'energy_units': 'MeV'},
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

    assert jdata['products']['analysis_parameters']['energ'] == 2000
    assert jdata['products']['analysis_parameters']['energy_units'] == 'keV'
    assert jdata['products']['echo']['energ'] == 2

    assert jdata['products']['analysis_parameters']['T1'] == '2017-03-06T13:26:48.000'
    assert jdata['products']['analysis_parameters']['T_format'] == 'isot'
    assert jdata['products']['echo']['T1'] == 57818.560277777775

@pytest.mark.fast
def test_unknown_argument(dispatcher_live_fixture):
    server = dispatcher_live_fixture   
    print("constructed server:", server)

    c = requests.get(server + "/run_analysis",
                   params={'instrument': 'empty',
                           'product_type': 'dummy',
                           'query_status': 'new',
                           'query_type': 'Real',
                           'unknown': 2.0},
                  )
    
    assert c.status_code == 200
    print("content:", c.text)
    jdata=c.json()
    
    assert re.match(r'Please note that arguments?.*unknown.*not used', jdata['exit_status']['comment'])
    assert 'T_format' not in jdata['exit_status']['comment']
    assert 'unknown' not in jdata['products']['analysis_parameters'].keys()
    assert 'unknown' not in jdata['products']['api_code']
    
@pytest.mark.fast
def test_catalog_selected_objects_accepted(dispatcher_live_fixture):
    server = dispatcher_live_fixture   
    print("constructed server:", server)

    c = requests.get(server + "/run_analysis",
                   params={'instrument': 'empty',
                           'product_type': 'dummy',
                           'query_status': 'new',
                           'query_type': 'Real',
                           'catalog_selected_objects': '0, 1'},
                  )
    
    assert c.status_code == 200
    print("content:", c.text)
    jdata=c.json()
    
    assert not re.match(r'Please note that arguments?.*catalog_selected_objects.*not used', jdata['exit_status']['comment'])
    assert 'catalog_selected_objects' in jdata['products']['analysis_parameters'].keys()
    assert 'catalog_selected_objects' in jdata['products']['api_code']
    
@pytest.mark.fast
def test_parameter_bounds_metadata(dispatcher_live_fixture):
    server = dispatcher_live_fixture   
    print("constructed server:", server)
    
    c = requests.get(server + '/meta-data',
                     params={'instrument': 'empty'})
    
    assert c.status_code == 200
    print("content:", c.text)
    jdata=c.json()
    
    metadata = [json.loads(x) for x in jdata[0] if isinstance(x, str)]
    if len(metadata) == 0:
        # new behaviour, metadata is not string-encoded in the request
        metadata = jdata[0]
    restricted_meta = [x for x in metadata if isinstance(x, list) and x[0]['query_name'] == 'restricted_parameters_dummy_query'][0]

    def meta_for_par(parname):
        return [x for x in restricted_meta if x.get('name', None) == parname][0]
    
    assert meta_for_par('bounded_int_par')['restrictions'] == {'is_optional': False, 'min_value': 2, 'max_value': 8}
    assert meta_for_par('bounded_float_par')['restrictions'] == {'is_optional': False, 'min_value': 2.2, 'max_value': 7.7}
    assert meta_for_par('string_select_par')['restrictions'] == {'is_optional': False, 'allowed_values': ['spam', 'eggs', 'ham']}
    
@pytest.mark.fast
def test_restricted_parameters_good_request(dispatcher_live_fixture):
    server = dispatcher_live_fixture   
    print("constructed server:", server)
    
    good_par = {'instrument': 'empty',
                'product_type': 'restricted',
                'query_status': 'new',
                'query_type': 'Real',
                'bounded_int_par': 6,
                'bounded_float_par': 6.1,
                'string_select_par': 'ham'
                }
    
    c = requests.get(server + '/run_analysis',
                     params = good_par)
    
    assert c.status_code == 200
    print("content:", c.text)
    jdata=c.json()
    assert jdata['exit_status']['status'] == 0
    assert jdata['exit_status']['job_status'] == 'done'
    # check parameters were actually set to proper values
    assert jdata['products']['echo']['bounded_int_par'] == 6
    assert jdata['products']['echo']['bounded_float_par'] == 6.1
    assert jdata['products']['echo']['string_select_par'] == 'ham'

@pytest.mark.fast
@pytest.mark.parametrize("par_name,par_value", (('bounded_int_par', 40), 
                                           ('bounded_float_par', -10.), 
                                           ('string_select_par', 'foo')))
def test_restricted_parameter_bad_request(dispatcher_live_fixture, par_name, par_value):
    server = dispatcher_live_fixture   
    print("constructed server:", server)
    
    good_par = {'instrument': 'empty',
                  'product_type': 'restricted',
                  'query_status': 'new',
                  'query_type': 'Real',
                  }
    
    c = requests.get(server + '/run_analysis',
                     params = {**good_par,
                               par_name: par_value})
    
    assert c.status_code == 400
    print("content:", c.text)
    jdata=c.json()
    assert jdata['error'].startswith( f'RequestNotUnderstood():Parameter {par_name} wrong value' )
    
@pytest.mark.fast
def test_structured_parameter(dispatcher_live_fixture):
    server = dispatcher_live_fixture   
    print("constructed server:", server)
    
    par = {'instrument': 'empty',
           'product_type': 'structured',
           'query_status': 'new',
           'query_type': 'Real',
           'struct': '{"b": [1, 2], "a": [4.2, 1.3]}',
           }
    
    c = requests.get(server + '/run_analysis',
                     params = par)
    
    assert c.status_code == 200
    print("content:", c.text)
    jdata=c.json()
    assert jdata['exit_status']['status'] == 0
    assert jdata['exit_status']['job_status'] == 'done'
    assert jdata['products']['echo']['struct'] == '{"a": [4.2, 1.3], "b": [1, 2]}'
    
    
@pytest.mark.fast
def test_malformed_structured_parameter(dispatcher_live_fixture):
    server = dispatcher_live_fixture   
    print("constructed server:", server)
    
    par = {'instrument': 'empty',
           'product_type': 'structured',
           'query_status': 'new',
           'query_type': 'Real',
           'struct': '{a: [4.2, 1.3]}',
           }
    
    c = requests.get(server + '/run_analysis',
                     params = par)
    #TODO:
    assert c.status_code == 400
    print("content:", c.text)
    jdata=c.json()
    assert 'Wrong value of structured parameter struct' in jdata['error_message']


@pytest.mark.fast
@pytest.mark.parametrize('par0', [None, 2.0, '\x00'])
@pytest.mark.parametrize('par1', [None, 3.0, '\x00'])
def test_optional_parameters(dispatcher_live_fixture, par0, par1):
    # NOTE: when request argument is None, it's just ignored by requests
    #       to set optional parameter to be empty (internally None), we use '\x00' in request

    server = dispatcher_live_fixture   
    print("constructed server:", server)
    
    c = requests.get(server + '/meta-data',
                     params = {'instrument': 'empty'})
    assert c.status_code == 200

    optional_query_meta = [q for q in c.json()[0][4:] if q[1]['product_name'] == "optional"][0]
    opt_pars = optional_query_meta[2:]

    for par in opt_pars:
        assert par['restrictions']['is_optional']

    defaults = {p['name']: p['value'] for p in opt_pars}
    expected = defaults.copy()
    if par0 is not None:
        expected[opt_pars[0]['name']] = None if par0=='\x00' else par0
    if par1 is not None:
        expected[opt_pars[1]['name']] = None if par1=='\x00' else par1
    
    req = {'instrument': 'empty',
           'product_type': 'optional',
           'query_status': 'new',
           'query_type': 'Real',
           opt_pars[0]['name']: par0,
           opt_pars[1]['name']: par1,
           }  
    c = requests.get(server + '/run_analysis',
                     params = req)
    
    assert c.status_code == 200
    print("content:", c.text)
    jdata=c.json()

    assert jdata['exit_status']['status'] == 0
    assert jdata['exit_status']['job_status'] == 'done'
    for k in expected.keys():
        assert jdata['products']['echo'][k] == expected[k]


@pytest.mark.fast
def test_nonoptional_parameter_is_not_nullable(dispatcher_live_fixture):
    
    server = dispatcher_live_fixture   
    print("constructed server:", server)

    req = {'instrument': 'empty',
           'product_type': 'numerical',
           'query_status': 'new',
           'query_type': 'Real',
           'p': '\x00'
           }  
    c = requests.get(server + '/run_analysis',
                     params = req)
    
    assert c.status_code == 400
    print("content:", c.text)
    jdata=c.json()

    assert jdata['error_message'] == 'Non-optional parameter p is set to None'
