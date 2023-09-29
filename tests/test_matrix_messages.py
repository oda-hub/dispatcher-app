import pytest
import requests
import json
import os
import time
import jwt
import logging

from cdci_data_analysis.pytest_fixtures import DispatcherJobState

logger = logging.getLogger(__name__)
# symmetric shared secret for the decoding of the token
secret_key = 'secretkey_test'

default_exp_time = int(time.time()) + 5000
default_token_payload = dict(
    sub="mtm@mtmco.net",
    mxroomid="!ftvnEnntbXuonXjiIB:matrix.org",
    name="mmeharga",
    roles="general",
    exp=default_exp_time,
    mxsub=True,
    mxintsub=5
)


@pytest.mark.test_matrix
@pytest.mark.parametrize("default_values", [True, False])
@pytest.mark.parametrize("time_original_request_none", [False])
@pytest.mark.parametrize("request_cred", ['public', 'private', 'private-no-matrix-message'])
def test_matrix_message_run_analysis_callback(gunicorn_dispatcher_long_living_fixture_with_matrix_options,
                                              default_values, request_cred, time_original_request_none):
    from cdci_data_analysis.plugins.dummy_plugin.data_server_dispatcher import DataServerQuery
    DataServerQuery.set_status('submitted')

    server = gunicorn_dispatcher_long_living_fixture_with_matrix_options

    DispatcherJobState.remove_scratch_folders()

    token_none = (request_cred == 'public')

    expect_matrix_message = True
    token_payload = {**default_token_payload,
                     "tmx": 0,
                     "tem": 0,
                     "mxstout": True,
                     "mxintsub": 5,
                     "mxsub": True,
                     "mssub": False,
                     "msdone": False,
                     "msfail": False
                     }

    if token_none:
        encoded_token = None
    else:
        # let's generate a valid token with high threshold

        if default_values:
            token_payload.pop('tmx')
            token_payload.pop('mxstout')
            token_payload.pop('mxsub')
            token_payload.pop('mxintsub')

        if request_cred == 'private-no-matrix-message':
            token_payload['mxsub'] = False
            token_payload['mxdone'] = False
            token_payload['mxfail'] = False
            expect_matrix_message = False

        encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        token=encoded_token
    )

    # this should return status submitted, so email sent
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )
    assert c.status_code == 200
    jdata = c.json()

    session_id = jdata['session_id']
    job_id = jdata['job_monitor']['job_id']

    logger.info("response from run_analysis: %s", json.dumps(jdata, indent=4))
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

    assert jdata['query_status'] == "submitted"

    completed_dict_param = {**dict_param,
                            'use_scws': 'no',
                            'src_name': '1E 1740.7-2942',
                            'RA': 265.97845833,
                            'DEC': -29.74516667,
                            'T1': '2017-03-06T13:26:48.000',
                            'T2': '2017-03-06T15:32:27.000',
                            'T_format': 'isot'
                            }

    products_url = DispatcherJobState.get_expected_products_url(completed_dict_param,
                                                                token=encoded_token,
                                                                session_id=session_id,
                                                                job_id=job_id)
    assert jdata['exit_status']['job_status'] == 'submitted'
    # get the original time the request was made
    assert 'time_request' in jdata
    # set the time the request was initiated
    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))

    if token_none or not expect_matrix_message:
        # email not supposed to be sent for public request
        assert 'matrix_message_status' not in jdata
    else:
        assert jdata['exit_status']['matrix_message_status'] == 'matrix message sent'

        # validate_email_content(
        #     dispatcher_local_mail_server.get_email_record(),
        #     'submitted',
        #     dispatcher_job_state,
        #     variation_suffixes=["dummy"],
        #     time_request_str=time_request_str,
        #     products_url=products_url,
        #     dispatcher_live_fixture=None,
        # )

    # for the call_back(s) in case the time of the original request is not provided
    if time_original_request_none:
        time_request = None
        time_request_str = 'None'

    for i in range(5):
        # imitating what a backend would do
        current_action = 'progress' if i > 2 else 'main_done'
        c = requests.get(os.path.join(server, "call_back"),
                         params=dict(
                             job_id=dispatcher_job_state.job_id,
                             session_id=dispatcher_job_state.session_id,
                             instrument_name="empty-async",
                             action=current_action,
                             node_id=f'node_{i}',
                             message='progressing',
                             token=encoded_token,
                             time_original_request=time_request
                         ))
        assert dispatcher_job_state.load_job_state_record(f'node_{i}', "progressing")['full_report_dict'][
                   'action'] == current_action

        c = requests.get(os.path.join(server, "run_analysis"),
                         params=dict(
                             query_status="submitted",  # whether query is new or not, this should work
                             query_type="Real",
                             instrument="empty-async",
                             product_type="dummy",
                             async_dispatcher=False,
                             session_id=dispatcher_job_state.session_id,
                             job_id=dispatcher_job_state.job_id,
                             token=encoded_token
                         ))
        assert c.json()['query_status'] == 'progress'  # always progress!

    # we should now find progress records
    c = requests.get(os.path.join(server, "run_analysis"),
                     {**dict_param,
                      "query_status": "submitted",
                      "job_id": job_id,
                      "session_id": session_id,
                      }
                     )

    assert c.status_code == 200
    jdata = c.json()

    assert len(jdata['job_monitor']['full_report_dict_list']) == 6
    assert [c['action'] for c in jdata['job_monitor']['full_report_dict_list']] == [
        'main_done', 'main_done', 'main_done', 'progress', 'progress', 'progress']

    c = requests.get(os.path.join(server, "call_back"),
                     params=dict(
                         job_id=dispatcher_job_state.job_id,
                         session_id=dispatcher_job_state.session_id,
                         instrument_name="empty-async",
                         action='main_incorrect_status',
                         node_id=f'node_{i + 1}',
                         message='progressing',
                         token=encoded_token,
                         time_original_request=time_request
                     ))
    assert c.status_code == 200

    c = requests.get(os.path.join(server, "run_analysis"),
                     {
                         **dict_param,
                         "query_status": "submitted",
                         "job_id": job_id,
                         "session_id": session_id,
                     }
                     )
    assert c.status_code == 200
    assert c.json()['query_status'] == 'progress'

    # this does nothing special
    c = requests.get(os.path.join(server, "call_back"),
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

    DataServerQuery.set_status('done')

    # this triggers email
    c = requests.get(os.path.join(server, "call_back"),
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
    jdata = dispatcher_job_state.load_job_state_record('node_final', 'done')

    if token_none or not expect_matrix_message:
        assert 'matrix_message_status' not in jdata

    elif time_original_request_none:
        assert 'matrix_message_status' in jdata

    elif default_values:
        assert 'matrix_message_status' not in jdata

    else:
        assert jdata['matrix_message_status'] == 'matrix message sent'

        # # check the email in the email folders, and that the first one was produced
        # dispatcher_job_state.assert_email(state="done")

        # # check the email in the log files
        # validate_email_content(
        #     dispatcher_local_mail_server.get_email_record(1),
        #     'done',
        #     dispatcher_job_state,
        #     time_request_str=time_request_str,
        #     dispatcher_live_fixture=server,
        # )

    # this also triggers email (simulate a failed request)
    c = requests.get(os.path.join(server, "call_back"),
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

    jdata = dispatcher_job_state.load_job_state_record('node_failed', 'failed')

    if token_none or not expect_matrix_message:
        # email not supposed to be sent for public request
        assert 'matrix_message_status' not in jdata
    else:
        assert jdata['matrix_message_status'] == 'matrix message sent'

        # # check the email in the email folders, and that the first one was produced
        # if default_values or time_original_request_none:
        #     dispatcher_job_state.assert_email('failed', comment="expected one email in total, failed")
        #     dispatcher_local_mail_server.assert_email_number(2)
        # else:
        #     dispatcher_job_state.assert_email('failed', comment="expected two emails in total, second failed")
        #     dispatcher_local_mail_server.assert_email_number(3)
        #
        # validate_email_content(
        #     dispatcher_local_mail_server.get_email_record(-1),
        #     'failed',
        #     dispatcher_job_state,
        #     time_request_str=time_request_str,
        #     dispatcher_live_fixture=server,
        # )

    # TODO this will rewrite the value of the time_request in the query output, but it shouldn't be a problem?
    # This is not complete since DataServerQuery never returns done
    c = requests.get(os.path.join(server, "run_analysis"),
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
