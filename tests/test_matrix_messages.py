import pytest
import requests
import json
import os
import time
import jwt
import logging
import re
import glob

from cdci_data_analysis.pytest_fixtures import DispatcherJobState

logger = logging.getLogger(__name__)
# symmetric shared secret for the decoding of the token
secret_key = 'secretkey_test'

default_exp_time = int(time.time()) + 5000
default_token_payload = dict(
    sub="mtm@mtmco.net",
    mxroomid="!eplWxXpvZcTgyQXhzC:matrix.org",
    user_id="@barni.gabriele:matrix.org",
    name="mmeharga",
    roles="general",
    exp=default_exp_time,
    mxsub=True,
    mxintsub=5
)


def validate_matrix_message_content(
        message_record,
        state: str,
        room_id:str,
        event_id:str,
        user_id:str,
        dispatcher_job_state: DispatcherJobState,
        time_request_str: str = None,
        products_url=None,
        dispatcher_live_fixture=None,
        request_params: dict = None,
        expect_api_code=True,
        variation_suffixes=None,
        require_reference_matrix_message=False
):
    if variation_suffixes is None:
        variation_suffixes = []

    if not expect_api_code:
        variation_suffixes.append("no-api-code")

    reference_matrix_message = get_reference_matrix_message(state=state,
                                                            time_request_str=time_request_str,
                                                            products_url=products_url,
                                                            job_id=dispatcher_job_state.job_id[:8],
                                                            variation_suffixes=variation_suffixes,
                                                            require=require_reference_matrix_message
                                                            )

    if request_params is None:
        request_params = {}
    product = request_params.get('product_type', 'dummy')

    assert message_record['room_id'] == room_id
    assert message_record['user_id'] == user_id
    assert message_record['type'] == 'm.room.message'
    assert message_record['event_id'] == event_id

    assert 'content' in message_record

    assert message_record['content']['format'] == 'org.matrix.custom.html'
    assert message_record['content']['msgtype'] == 'm.text'

    assert re.search(f'Dear User', message_record['content']['body'], re.IGNORECASE)
    assert re.search(f'Kind Regards', message_record['content']['body'], re.IGNORECASE)

    if reference_matrix_message is not None:
        assert (DispatcherJobState.ignore_html_patterns(reference_matrix_message) ==
                DispatcherJobState.ignore_html_patterns(message_record['content']['formatted_body']))

    if products_url is not None:
        if products_url != "":
            assert re.search(f'<a href="(.*)">.*?</a>', message_record['content']['formatted_body'], re.M).group(1) == products_url
            DispatcherJobState.validate_products_url(
                DispatcherJobState.extract_products_url(message_record['content']['formatted_body']),
                dispatcher_live_fixture,
                product_type=product
            )
            assert products_url in message_record['content']['body']
        else:
            assert re.search(f'<a href="(.*)">url</a>', message_record['content']['formatted_body'], re.M) is None

    if expect_api_code:
        DispatcherJobState.validate_api_code(
            DispatcherJobState.extract_api_code_from_text(message_record['content']['formatted_body']),
            dispatcher_live_fixture,
            product_type=product
        )
    else:
        assert "Please note the API code for this query was too large to embed it in the email text. Instead," \
               " we attach it as a python script." in message_record['content']['body']


def matrix_message_args_to_filename(**matrix_message_args):
    suffix = "-".join(matrix_message_args.get('variation_suffixes', []))

    if suffix != "":
        suffix = "-" + suffix

    fn = "tests/{matrix_message_collection}_matrix_messages/{state}{suffix}.html".format(suffix=suffix, **matrix_message_args)
    os.makedirs(os.path.dirname(fn), exist_ok=True)
    return fn


def get_reference_matrix_message(**matrix_message_args):
    fn = os.path.abspath(matrix_message_args_to_filename(**{**matrix_message_args, 'matrix_message_collection': 'reference'}))
    try:
        html_content = open(fn).read()
        return adapt_html(html_content, **matrix_message_args)
    except FileNotFoundError:
        if matrix_message_args.get('require', False):
            raise
        else:
            return None


def adapt_html(html_content, patterns=None, **matrix_message_args,):
    if patterns is None:
        patterns = DispatcherJobState.generalized_patterns
    for arg, patterns in patterns.items():
        if arg in matrix_message_args and matrix_message_args[arg] is not None:
            for pattern in patterns:
                html_content = re.sub(pattern, r"\g<1>" + matrix_message_args[arg] + r"\g<3>", html_content)

    return html_content


@pytest.mark.test_matrix
@pytest.mark.parametrize("default_values", [True, False])
@pytest.mark.parametrize("time_original_request_none", [False])
@pytest.mark.parametrize("request_cred", ['public', 'private', 'private-no-matrix-message'])
def test_matrix_message_run_analysis_callback(gunicorn_dispatcher_long_living_fixture_with_matrix_options,
                                              dispatcher_local_matrix_message_server,
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
                     "msfail": False,
                     "mxroomid": dispatcher_local_matrix_message_server.room_id
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
        assert 'matrix_message_status' in jdata['exit_status']
        assert jdata['exit_status']['matrix_message_status'] == 'matrix message sent'
        assert 'matrix_message_status_details' in jdata['exit_status']
        matrix_message_event_id_obj = json.loads(jdata['exit_status']['matrix_message_status_details'])
        assert 'event_id' in matrix_message_event_id_obj['res_content']

        matrix_message_event_id_obj = matrix_message_event_id_obj['res_content']['event_id']

        validate_matrix_message_content(
            dispatcher_local_matrix_message_server.get_matrix_message_record(room_id=token_payload['mxroomid'],
                                                                             event_id=matrix_message_event_id_obj),
            'submitted',
            room_id=token_payload['mxroomid'],
            event_id=matrix_message_event_id_obj,
            user_id=token_payload['user_id'],
            dispatcher_job_state=dispatcher_job_state,
            variation_suffixes=["dummy"],
            time_request_str=time_request_str,
            request_params=dict_param,
            products_url=products_url,
            dispatcher_live_fixture=None,
            require_reference_matrix_message=True
        )

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

    # this triggers a message via matrix
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

    jdata = dispatcher_job_state.load_job_state_record('node_final', 'done')

    if token_none or not expect_matrix_message:
        assert 'matrix_message_status' not in jdata

    elif time_original_request_none:
        assert 'matrix_message_status' in jdata

    elif default_values:
        assert 'matrix_message_status' not in jdata

    else:
        assert 'matrix_message_status' in jdata
        assert jdata['matrix_message_status'] == 'matrix message sent'
        assert 'matrix_message_status_details' in jdata
        matrix_message_event_id_obj = json.loads(jdata['matrix_message_status_details'])
        assert 'event_id' in matrix_message_event_id_obj['res_content']

        matrix_message_event_id_obj = matrix_message_event_id_obj['res_content']['event_id']
        # check the email in the email folders, and that the first one was produced
        dispatcher_job_state.assert_matrix_message(state="done")

        validate_matrix_message_content(
            dispatcher_local_matrix_message_server.get_matrix_message_record(room_id=token_payload['mxroomid'],
                                                                              event_id=matrix_message_event_id_obj),
            'done',
            room_id=token_payload['mxroomid'],
            event_id=matrix_message_event_id_obj,
            user_id=token_payload['user_id'],
            dispatcher_job_state=dispatcher_job_state,
            time_request_str=time_request_str,
            dispatcher_live_fixture=server,
            require_reference_matrix_message=True
        )

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
        assert 'matrix_message_status_details' not in jdata
    else:
        assert 'matrix_message_status' in jdata
        assert jdata['matrix_message_status'] == 'matrix message sent'
        assert 'matrix_message_status_details' in jdata
        matrix_message_event_id_obj = json.loads(jdata['matrix_message_status_details'])
        assert 'event_id' in matrix_message_event_id_obj['res_content']

        matrix_message_event_id_obj = matrix_message_event_id_obj['res_content']['event_id']

        # check the email in the email folders, and that the first one was produced
        if default_values or time_original_request_none:
            dispatcher_job_state.assert_matrix_message('failed', comment="expected one email in total, failed")
        else:
            dispatcher_job_state.assert_matrix_message('failed', comment="expected two emails in total, second failed")

        validate_matrix_message_content(
            dispatcher_local_matrix_message_server.get_matrix_message_record(room_id=token_payload['mxroomid'],
                                                                             event_id=matrix_message_event_id_obj),
            'failed',
            room_id=token_payload['mxroomid'],
            event_id=matrix_message_event_id_obj,
            user_id=token_payload['user_id'],
            dispatcher_job_state=dispatcher_job_state,
            time_request_str=time_request_str,
            dispatcher_live_fixture=server,
            require_reference_matrix_message=True
        )


@pytest.mark.test_matrix
@pytest.mark.not_safe_parallel
def test_matrix_message_submitted_same_job(dispatcher_live_fixture_with_matrix_options,
                                           dispatcher_local_matrix_message_server):
    # remove all the current scratch folders
    DispatcherJobState.remove_scratch_folders()

    server = dispatcher_live_fixture_with_matrix_options
    logger.info("constructed server: %s", server)

    token_payload = {**default_token_payload,
                     "mxintsub": 20,
                     "mxstout": True,
                     "mxroomid": dispatcher_local_matrix_message_server.room_id
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
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    assert c.status_code == 200

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'

    assert 'matrix_message_status' in jdata['exit_status']
    assert jdata['exit_status']['matrix_message_status'] == 'matrix message sent'
    assert 'matrix_message_status_details' in jdata['exit_status']
    matrix_message_event_id_obj = json.loads(jdata['exit_status']['matrix_message_status_details'])
    assert 'event_id' in matrix_message_event_id_obj['res_content']

    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))

    dispatcher_job_state.assert_matrix_message(state="submitted")

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
        c = requests.get(os.path.join(server, "run_analysis"),
                         dict_param
                         )

        assert c.status_code == 200
        jdata = c.json()
        assert jdata['exit_status']['job_status'] == 'submitted'
        assert 'matrix_message_status' not in jdata['exit_status']

        # check the matrix message in the matrix messages folders, and that only the first one was produced
        dispatcher_job_state.assert_matrix_message(state="submitted", number=1)

    # let the interval time pass, so that a new message is sent on matrix
    time.sleep(20)
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    assert c.status_code == 200
    jdata = c.json()

    try:
        assert jdata['exit_status']['job_status'] == 'submitted'
        assert 'matrix_message_status' in jdata['exit_status']
        assert jdata['exit_status']['matrix_message_status'] == 'matrix message sent'
    except KeyError:
        logger.error(json.dumps(jdata, indent=4, sort_keys=True))
        raise

    # check the matrix message in the matrix messages folders, and that a second one has been sent
    dispatcher_job_state.assert_matrix_message(state="submitted", number=2)

    list_matrix_message_files = glob.glob(os.path.join(dispatcher_job_state.matrix_message_history_folder, f'matrix_message_submitted_*.json'))
    assert len(list_matrix_message_files) == 2
    for matrix_message_file in list_matrix_message_files:
        f_name, f_ext = os.path.splitext(os.path.basename(matrix_message_file))
        f_name_splited = f_name.split('_')
        assert len(f_name_splited) == 5
        assert float(f_name.split('_')[4]) == time_request

    # let the interval time pass again, so that a new message is sent on matrix
    time.sleep(20)
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    assert c.status_code == 200
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'

    assert 'matrix_message_status' in jdata['exit_status']
    assert jdata['exit_status']['matrix_message_status'] == 'matrix message sent'

    list_matrix_message_files = glob.glob(os.path.join(dispatcher_job_state.matrix_message_history_folder, f'matrix_message_submitted_*.json'))
    assert len(list_matrix_message_files) == 3
    for email_file in list_matrix_message_files:
        f_name, f_ext = os.path.splitext(os.path.basename(email_file))
        f_name_splited = f_name.split('_')
        assert len(f_name_splited) == 5
        assert float(f_name.split('_')[4]) == time_request


@pytest.mark.test_matrix
@pytest.mark.not_safe_parallel
def test_matrix_message_submitted_frontend_like_job_id(dispatcher_live_fixture_with_matrix_options,
                                                       dispatcher_local_matrix_message_server):
    DispatcherJobState.remove_scratch_folders()

    server = dispatcher_live_fixture_with_matrix_options
    logger.info("constructed server: %s", server)

    token_payload = {**default_token_payload,
                     "mxroomid": dispatcher_local_matrix_message_server.room_id
                     }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        token=encoded_token,
        job_id=""
    )

    # this should return status submitted, so a message on matrix sent
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    assert c.status_code == 200

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert 'matrix_message_status' in jdata['exit_status']
    assert jdata['exit_status']['matrix_message_status'] == 'matrix message sent'

    dispatcher_job_state.assert_matrix_message(state="submitted")
