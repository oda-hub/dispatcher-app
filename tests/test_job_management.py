from typing import OrderedDict
import pytest
import requests
import json
import os
import re
import time
import jwt
import logging
import email
from urllib.parse import urlencode
import glob

from cdci_data_analysis.pytest_fixtures import DispatcherJobState

from flask import Markup

logger = logging.getLogger(__name__)
# symmetric shared secret for the decoding of the token
secret_key = 'secretkey_test'

default_exp_time = int(time.time()) + 5000
default_token_payload = dict(
    sub="mtm@mtmco.net",
    name="mmeharga",
    roles="general",
    exp=default_exp_time,
    tem=0,
    mstout=True,
    mssub=True,
    intsub=5
)


def test_callback_without_prior_run_analysis(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    c = requests.get(server + "/call_back",
                     params={
                         'job_id': 'test-job-id',
                         'instrument_name': 'test-instrument_name',
                     })

    logger.info(c.text)

    assert c.status_code == 200


def test_public_async_request(dispatcher_live_fixture, dispatcher_local_mail_server):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy"
    )

    # this should return status submitted, so email sent
    c = requests.get(server + "/run_analysis",
                     dict_param
                     )

    logger.info("response from run_analysis: %s", json.dumps(c.json(), indent=4))

    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert 'email_status' not in jdata['exit_status']

    session_id = c.json()['session_id']
    job_id = c.json()['job_monitor']['job_id']

    c = requests.get(server + "/run_analysis",
                     params=dict(
                         query_status="ready",  # whether query is new or not, this should work
                         query_type="Real",
                         instrument="empty-async",
                         product_type="dummy",
                         async_dispatcher=False,
                         session_id=session_id,
                         job_id=job_id,
                     ))

    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert 'email_status' not in jdata['exit_status']



def validate_email_content(
                   message_record, 
                   state: str,
                   dispatcher_job_state: DispatcherJobState,
                   time_request_str: str=None,
                   products_url=None,
                   ):
    
    assert message_record['mail_from'] == 'team@odahub.io'
    assert message_record['rcpt_tos'] == ['mtm@mtmco.net', 'team@odahub.io']

    msg = email.message_from_string(message_record['data'])    

    assert msg['Subject'] == f"[ODA][{state}] dummy first requested at {time_request_str} job_id: {dispatcher_job_state.job_id[:8]}"
    assert msg['From'] == 'team@odahub.io'
    assert msg['To'] == 'mtm@mtmco.net'
    assert msg['CC'] == ", ".join(['team@odahub.io'])
    assert msg.is_multipart()
    
    for part in msg.walk():
        content_text = None

        if part.get_content_type() == 'text/plain':
            content_text_plain = part.get_payload().replace('\r', '').strip()
            content_text = content_text_plain                        
        elif part.get_content_type() == 'text/html':
            content_text_html = part.get_payload().replace('\r', '').strip()
            content_text = content_text_html

            if products_url is not None:                
                assert re.search(f'<a href="(.*)">.*?</a>', content_text_html, re.M).group(1) == products_url

        if content_text is not None:
            assert re.search(f'Dear User', content_text, re.IGNORECASE)
            assert re.search(f'Kind Regards', content_text, re.IGNORECASE)

            if products_url is not None:                
                assert products_url in content_text




@pytest.mark.parametrize("default_values", [True, False])
@pytest.mark.parametrize("time_original_request_none", [False])
#why is it None sometimes, and should we really send an email in this case?..
#@pytest.mark.parametrize("time_original_request_none", [True, False])
@pytest.mark.parametrize("request_cred", ['public', 'private'])
def test_email_callback_after_run_analysis(dispatcher_long_living_fixture, dispatcher_local_mail_server, default_values, request_cred, time_original_request_none):
    # TODO: for now, this is not very different from no-prior-run_analysis. This will improve
    
    token_none = ( request_cred == 'public' )
        
    server = dispatcher_long_living_fixture
    
    if token_none:
        encoded_token = None
    else:
        # let's generate a valid token with high threshold
        token_payload = {
            **default_token_payload,
            "tem": 0
        }

        if default_values:
            token_payload.pop('tem')
            token_payload.pop('mstout')
            token_payload.pop('mssub')
            token_payload.pop('intsub')

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

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c)

    dict_param_complete = dict_param.copy()    
    dict_param_complete.pop("token")

    assert 'session_id' not in dict_param_complete
    assert 'job_id' not in dict_param_complete
    assert 'token' not in dict_param_complete

    products_url = '%s?%s' % ('http://www.astro.unige.ch/cdci/astrooda_', urlencode(dict_param_complete))
    
        
    assert jdata['exit_status']['job_status'] == 'submitted'
    # get the original time the request was made
    assert 'time_request' in jdata
    # set the time the request was initiated
    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))
    
    if token_none:
        # email not supposed to be sent for public request
        assert 'email_status' not in jdata
    else:
        assert jdata['exit_status']['email_status'] == 'email sent'
        
        validate_email_content(
            dispatcher_local_mail_server.get_email_record(),
            'submitted',
            dispatcher_job_state,
            time_request_str=time_request_str,
            products_url=products_url,
        )
        
    # for the call_back(s) in case the time of the original request is not provided
    if time_original_request_none:
        time_request = None
        time_request_str = 'None'
        
    for i in range(5):
        # imitating what a backend would do
        c = requests.get(server + "/call_back",
                         params=dict(
                             job_id=dispatcher_job_state.job_id,
                             session_id=dispatcher_job_state.session_id,
                             instrument_name="empty-async",
                             action='progress',
                             node_id=f'node_{i}',
                             message='progressing',
                             token=encoded_token,
                             time_original_request=time_request
                         ))

    c = requests.get(server + "/call_back",
                     params=dict(
                         job_id=dispatcher_job_state.job_id,
                         session_id=dispatcher_job_state.session_id,
                         instrument_name="empty-async",
                         action='ready',
                         node_id='node_ready',
                         message='ready',
                         token=encoded_token,
                         time_original_request=time_request
                     ))

    # this triggers email
    c = requests.get(server + "/call_back",
                     params=dict(
                         job_id=dispatcher_job_state.job_id,
                         session_id=dispatcher_job_state.session_id,
                         instrument_name="empty-async",
                         action='done',
                         node_id='node_final',
                         message='done',
                         token=encoded_token,
                         time_original_request=time_request
                     ))
    assert c.status_code == 200

    # TODO build a test that effectively test both paths
    jdata = dispatcher_job_state.load_job_state_record('final', 'done')    
        
    # if default_values or token_none or time_request_none:
    if default_values or token_none or time_original_request_none:
        # for this case, email not supposed to be sent if request is short and/or no time information are available
        # or public request
        assert 'email_status' not in jdata
    else:
        assert jdata['email_status'] == 'email sent'

        # check the email in the email folders, and that the first one was produced
        dispatcher_job_state.assert_email(1, state="done")
        
        # check the email in the log files
        validate_email_content(
            dispatcher_local_mail_server.get_email_record(1),
            'done',
            dispatcher_job_state,
            time_request_str=time_request_str,
        )
        
    # this also triggers email (simulate a failed request)
    c = requests.get(server + "/call_back",
                     params={
                         'job_id': dispatcher_job_state.job_id,
                         'session_id': dispatcher_job_state.session_id,
                         'instrument_name': "empty-async",
                         'action': 'failed',
                         'node_id': 'node_failed',
                         'message': 'failed',
                         'token': encoded_token,
                         'time_original_request': time_request
                     })

    assert c.status_code == 200

    jdata = dispatcher_job_state.load_job_state_record('failed', 'failed')

    if token_none:
        # email not supposed to be sent for public request
        assert 'email_status' not in jdata
    else:
        assert jdata['email_status'] == 'email sent'

        # check the email in the email folders, and that the first one was produced        
        if default_values or time_original_request_none:
            dispatcher_job_state.assert_email(1, 'failed')                        
        else:
            dispatcher_job_state.assert_email(2, 'failed')                        
        
        # comment why does this produce less emails?
        if default_values or time_original_request_none:
            dispatcher_local_mail_server.assert_email_number(2)
        else:
            dispatcher_local_mail_server.assert_email_number(3)
 
        validate_email_content(
            dispatcher_local_mail_server.get_email_record(-1),
            'failed',
            dispatcher_job_state,
            time_request_str=time_request_str,
        )

    # TODO this will rewrite the value of the time_request in the query output, but it shouldn't be a problem?
    # This is not complete since DataServerQuery never returns done
    c = requests.get(server + "/run_analysis",
                     params=dict(
                         query_status="ready",  # whether query is new or not, this should work
                         query_type="Real",
                         instrument="empty-async",
                         product_type="dummy",
                         async_dispatcher=False,
                         session_id=dispatcher_job_state.session_id,
                         job_id=dispatcher_job_state.job_id,
                         token=encoded_token
                     ))

    logger.info("response from run_analysis: %s", json.dumps(c.json(), indent=4))    

    assert c.status_code == 200

    # TODO: test that this returns the result

def test_email_submitted(dispatcher_live_fixture, dispatcher_local_mail_server):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # email content in plain text and html format
    smtp_server_log = dispatcher_local_mail_server.local_smtp_output_json_fn

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "tem": 0,
        "intsub": 3
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
    
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c)
    
    #dict_param_complete = dict_param.copy()
    #dict_param_complete.pop("token")


    assert c.status_code == 200
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert jdata['exit_status']['email_status'] == 'email sent'

    # check the email in the email folders, and that the first one was produced
    
    dispatcher_job_state.assert_email(serial_number=0, state="submitted")
    dispatcher_local_mail_server.assert_email_number(1)

    
    # re-submit the very same request, in order to produce a sequence of submitted status
    # and verify not a sequence of emails are generated
    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        session_id=dispatcher_job_state.session_id,
        job_id=dispatcher_job_state.job_id,
        token=encoded_token
    )

    for i in range(3):
        c = requests.get(server + "/run_analysis",
                         dict_param
                         )

        assert c.status_code == 200
        jdata = c.json()
        assert jdata['exit_status']['job_status'] == 'submitted'
        assert 'email_status' not in jdata['exit_status']

        # check the email in the email folders, and that the first one was produced
        dispatcher_job_state.assert_email(serial_number=1, state="submitted", number=0)
        dispatcher_local_mail_server.assert_email_number(1)

    # let the interval time pass, so that a new email si sent
    time.sleep(5)
    c = requests.get(server + "/run_analysis",
                     dict_param
                     )

    assert c.status_code == 200
    jdata = c.json()
    
    try:
        assert jdata['exit_status']['job_status'] == 'submitted'
        assert jdata['exit_status']['email_status'] == 'email sent'
    except KeyError:
        logger.error(json.dumps(jdata, indent=4, sort_keys=True))
        raise

    # check the email in the email folders, and that the first one was produced
    
    dispatcher_job_state.assert_email(serial_number=1, state="submitted", number=1)
    dispatcher_local_mail_server.assert_email_number(2)
    


def test_email_failure_callback_after_run_analysis(dispatcher_live_fixture):
    # TODO: for now, this is not very different from no-prior-run_analysis. This will improve

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "tem": 0
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    # set the time the request was initiated
    time_request = time.time()
    c = requests.get(server + "/run_analysis",
                     params=dict(
                         query_status="new",
                         query_type="Real",
                         instrument="empty-async",
                         product_type="dummy",
                         token=encoded_token,
                         time_request=time_request
                     ))

    logger.info("response from run_analysis: %s", json.dumps(c.json(), indent=4))

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c)    

    jdata = c.json()
    assert jdata['exit_status']['email_status'] == 'sending email failed'

    # this triggers email
    c = requests.get(server + "/call_back",
                     params={
                         'job_id': dispatcher_job_state.job_id,
                         'session_id': dispatcher_job_state.session_id,
                         'instrument_name': "empty-async",
                         'action': 'failed',
                         'node_id': 'node_failed',
                         'message': 'failed',
                         'token': encoded_token,
                         'time_original_request': time_request
                     })
    assert c.status_code == 200

    job_monitor_call_back_failed_json_fn = f'{dispatcher_job_state.scratch_dir}/job_monitor_node_failed_failed_.json'
    
    jdata = json.load(open(job_monitor_call_back_failed_json_fn))
    
    assert jdata['email_status'] == 'sending email failed'
    assert not os.path.exists(dispatcher_job_state.email_history_folder)


def test_email_callback_after_run_analysis_subprocess_mail_server(dispatcher_live_fixture, dispatcher_local_mail_server_subprocess):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "tem": 0,
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    # this should return status submitted, so email sent
    c = requests.get(server + "/run_analysis",
                     params=dict(
                         query_status="new",
                         query_type="Real",
                         instrument="empty-async",
                         product_type="dummy",
                         token=encoded_token
                     ))

    logger.info("response from run_analysis: %s", json.dumps(c.json(), indent=4))

    session_id = c.json()['session_id']
    job_id = c.json()['job_monitor']['job_id']

    job_monitor_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/job_monitor.json'
    # the aliased version might have been created
    job_monitor_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/job_monitor.json'

    assert os.path.exists(job_monitor_json_fn) or os.path.exists(job_monitor_json_fn_aliased)
    assert c.status_code == 200

    # read the json file and get the path for the email history
    if os.path.exists(job_monitor_json_fn):
        email_history_folder_path = f'scratch_sid_{session_id}_jid_{job_id}/email_history'
    else:
        email_history_folder_path = f'scratch_sid_{session_id}_jid_{job_id}_aliased/email_history'

    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert jdata['exit_status']['email_status'] == 'email sent'

    assert os.path.exists(email_history_folder_path)
    list_email_files = glob.glob(email_history_folder_path + '/email_0_*.email')
    assert len(list_email_files) == 1
