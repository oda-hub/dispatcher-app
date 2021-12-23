import requests
import time
import json
import os
import logging
import jwt
import glob
import pytest
from datetime import datetime
from functools import reduce
import yaml
import gzip

from cdci_data_analysis.analysis.catalog import BasicCatalog
from cdci_data_analysis.pytest_fixtures import DispatcherJobState, ask, make_hash
from cdci_data_analysis.flask_app.dispatcher_query import InstrumentQueryBackEnd


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
    assert jdata['installed_instruments'] == ['empty', 'empty-async', 'empty-semi-async'] or \
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


@pytest.mark.parametrize("instrument", ["", "None", None, "undefined"])
def test_download_products_public(dispatcher_live_fixture, empty_products_files_fixture, instrument):    
    server = dispatcher_live_fixture

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

    # this should return status submitted, so email sent
    c = requests.get(server + "/run_analysis",
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

    c = requests.get(server + "/call_back",
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

    mapper = lambda x, y: ".".join(map(str, x))
    example_config_keys = flatten_nested_structure(example_config, mapper)
    test_config_keys = flatten_nested_structure(dispatcher_test_conf, mapper)

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
def test_product_gallery_post_article(dispatcher_live_fixture):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    # send simple request
    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "general, unige-hpc-full",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **default_params,
        'src_name': 'Mrk 421',
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
                )

    job_id = jdata['products']['job_id']
    session_id = jdata['session_id']
    product_title = "_".join([params['instrument'], params['query_type'], datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")])
    params = {
        'job_id': job_id,
        'session_id': session_id,
        'observation_id': 'test observation',
        'content_type': 'data_product',
        'E1_keV': 45,
        'E2_kev': 95,
        'DEC': 145,
        'RA': 95.23,
        'token': encoded_token
    }

    # send test img
    img_file_obj = {'media': open('data/dummy_prods/ds9.jpeg', 'rb')}

    c = requests.post(server + "/post_product_to_gallery",
                      params={**params},
                      files=img_file_obj
                      )

    assert c.status_code == 200
