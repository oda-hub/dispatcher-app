import requests
import json
import os
import pytest

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


def test_callback_after_run_analysis(dispatcher_live_fixture):
    #TODO: for now, this is not very different from no-prior-run_analysis. This will improve

    server = dispatcher_live_fixture
    print("constructed server:", server)

    c = requests.get(server + "/run_analysis",
                     params=dict(
                        query_status="new",
                        query_type="Real",
                        instrument="empty-async",
                        product_type="dummy",                        
                        async_dispatcher=False,
                    ))

    print("response from run_analysis:", json.dumps(c.json(), indent=4))

    session_id = c.json()['session_id']
    job_id = c.json()['job_monitor']['job_id']

    job_monitor_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/job_monitor.json'

    assert os.path.exists(job_monitor_json_fn)

    assert c.status_code == 200        

    for i in range(3):
        c = requests.get(server + "/call_back",
                    params={
                        'job_id': job_id,
                        'session_id': session_id,
                        'instrument_name': "empty-async",
                        'action': 'progress',
                        'node_id': f'node_{i}',
                        'message': 'progressing',
                    })

    # this should trigger email
    c = requests.get(server + "/call_back",
                params={
                    'job_id': job_id,
                    'session_id': session_id,
                    'instrument_name': "empty-async",
                    'action': 'ready',
                    'node_id': 'final',
                    'message': 'done',
                })

    print(c.text)    

    c = requests.get(server + "/run_analysis",
                     params=dict(
                        query_status="ready", # whether query is new or not, this should work
                        query_type="Real",
                        instrument="empty-async",
                        product_type="dummy",                        
                        async_dispatcher=False,
                        session_id=session_id,
                        job_id=job_id,
                    ))

    print("response from run_analysis:", json.dumps(c.json(), indent=4))

    assert c.status_code == 200
