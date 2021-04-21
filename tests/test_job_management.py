import requests
import json
import os
import time
import jwt
import logging


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
)


def test_callback_without_prior_run_analysis(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    c = requests.get(server + "/call_back",
                     params={
                         'job_id': 'test-job-id',
                         'instrument_name': 'test-instrument_name',
                     },
                     )

    print(c.text)

    assert c.status_code == 200


def test_email_callback_after_run_analysis(dispatcher_live_fixture, dispatcher_local_mail_server):
    # TODO: for now, this is not very different from no-prior-run_analysis. This will improve

    server = dispatcher_live_fixture
    print("constructed server:", server)

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "tem": 0,
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    # set the time the request was initiated
    time_request = time.time()
    # this should return status submitted, so email sent
    c = requests.get(server + "/run_analysis",
                     params=dict(
                         query_status="new",
                         query_type="Real",
                         instrument="empty-async",
                         product_type="dummy",
                         token=encoded_token
                     ))

    print("response from run_analysis:", json.dumps(c.json(), indent=4))

    session_id = c.json()['session_id']
    job_id = c.json()['job_monitor']['job_id']

    job_monitor_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/job_monitor.json'
    # the aliased version might have been created
    job_monitor_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/job_monitor.json'

    assert os.path.exists(job_monitor_json_fn) or os.path.exists(job_monitor_json_fn_aliased)
    assert c.status_code == 200

    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert jdata['exit_status']['email_status'] == 'email sent'

    for i in range(5):
        # imitating what a backend would do
        c = requests.get(server + "/call_back",
                         params=dict(
                             job_id=job_id,
                             session_id=session_id,
                             instrument_name="empty-async",
                             action='progress',
                             node_id=f'node_{i}',
                             message='progressing',
                             token=encoded_token,
                             time_request=time_request
                         ))

    c = requests.get(server + "/call_back",
                     params=dict(
                         job_id=job_id,
                         session_id=session_id,
                         instrument_name="empty-async",
                         action='ready',
                         node_id='node_ready',
                         message='ready',
                         token=encoded_token,
                         time_request=time_request
                     ))

    # this triggers email
    c = requests.get(server + "/call_back",
                     params=dict(
                         job_id=job_id,
                         session_id=session_id,
                         instrument_name="empty-async",
                         action='done',
                         node_id='node_final',
                         message='done',
                         token=encoded_token,
                         time_request=time_request
                     ))

    job_monitor_call_back_done_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/job_monitor_node_final_done_.json'
    # the aliased version might have been created
    job_monitor_call_back_done_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/job_monitor_node_final_done_.json'
    assert os.path.exists(job_monitor_call_back_done_json_fn) or \
           os.path.exists(job_monitor_call_back_done_json_fn_aliased)
    assert c.status_code == 200
    # read the json file
    if os.path.exists(job_monitor_call_back_done_json_fn):
        f = open(job_monitor_call_back_done_json_fn)
    else:
        f = open(job_monitor_call_back_done_json_fn_aliased)

    jdata = json.load(f)
    assert jdata['email_status'] == 'email sent'

    # this also triggers email (simulate a failed request)
    c = requests.get(server + "/call_back",
                     params={
                         'job_id': job_id,
                         'session_id': session_id,
                         'instrument_name': "empty-async",
                         'action': 'failed',
                         'node_id': 'node_failed',
                         'message': 'failed',
                         'token': encoded_token,
                         'time_request': time_request
                     })
    job_monitor_call_back_failed_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/job_monitor_node_failed_failed_.json'
    # the aliased version might have been created
    job_monitor_call_back_failed_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/job_monitor_node_failed_failed_.json'

    assert os.path.exists(job_monitor_call_back_failed_json_fn) or os.path.exists(
        job_monitor_call_back_failed_json_fn_aliased)
    assert c.status_code == 200
    # read the json file
    if os.path.exists(job_monitor_call_back_failed_json_fn):
        f = open(job_monitor_call_back_failed_json_fn)
    else:
        f = open(job_monitor_call_back_failed_json_fn_aliased)

    jdata = json.load(f)
    assert jdata['email_status'] == 'email sent'


    # This is not complete since DataServerQuery never returns done
    c = requests.get(server + "/run_analysis",
                     params=dict(
                         query_status="ready",  # whether query is new or not, this should work
                         query_type="Real",
                         instrument="empty-async",
                         product_type="dummy",
                         async_dispatcher=False,
                         session_id=session_id,
                         job_id=job_id,
                         token=encoded_token
                     ))

    print("response from run_analysis:", json.dumps(c.json(), indent=4))
    # jdata = c.json()
    # TODO: test that this returns entire log
    # full_report_dict_list = c.json()['job_monitor'].get('full_report_dict_list')
    # assert len(full_report_dict_list) == 5

    assert c.status_code == 200

    # TODO: test that this returns the result


def test_email_failure_callback_after_run_analysis(dispatcher_live_fixture):
    # TODO: for now, this is not very different from no-prior-run_analysis. This will improve

    server = dispatcher_live_fixture
    print("constructed server:", server)

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

    print("response from run_analysis:", json.dumps(c.json(), indent=4))

    session_id = c.json()['session_id']
    job_id = c.json()['job_monitor']['job_id']

    # TODO ensure it is submitted

    job_monitor_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/job_monitor.json'
    # the aliased version might have been created
    job_monitor_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/job_monitor.json'

    assert os.path.exists(job_monitor_json_fn) or os.path.exists(job_monitor_json_fn_aliased)
    assert c.status_code == 200

    jdata = c.json()
    assert jdata['exit_status']['email_status'] == 'sending email failed'

    # this triggers email
    c = requests.get(server + "/call_back",
                     params={
                         'job_id': job_id,
                         'session_id': session_id,
                         'instrument_name': "empty-async",
                         'action': 'failed',
                         'node_id': 'node_failed',
                         'message': 'failed',
                         'token': encoded_token,
                         'time_request': time_request
                     })
    job_monitor_call_back_failed_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/job_monitor_node_failed_failed_.json'
    # the aliased version might have been created
    job_monitor_call_back_failed_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/job_monitor_node_failed_failed_.json'

    assert os.path.exists(job_monitor_call_back_failed_json_fn) or os.path.exists(
        job_monitor_call_back_failed_json_fn_aliased)
    assert c.status_code == 200
    # read the json file
    if os.path.exists(job_monitor_call_back_failed_json_fn):
        f = open(job_monitor_call_back_failed_json_fn)
    else:
        f = open(job_monitor_call_back_failed_json_fn_aliased)

    jdata = json.load(f)
    assert jdata['email_status'] == 'sending email failed'
