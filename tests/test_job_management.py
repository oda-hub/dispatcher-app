import shutil
from urllib import parse

import pytest
import requests
import json
import os
import re
import time
import jwt
import base64
import zlib
import logging
import email
from urllib.parse import urlencode
import glob

from cdci_data_analysis.pytest_fixtures import DispatcherJobState, make_hash

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


# we want to be able to view, in browser, fully formed emails. So storing templates does not do.
# but every test will generate them a bit differently, due to time embedded in them
# so just this time recored should be adapted for every test

generalized_email_patterns = {
    'time_request_str': [
        r'(because at )([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.*?)( \()',
        '(first requested at )(.*? .*?)( job_id:)'
    ],
    'products_url': [
        '(href=")(.*?)(">url)',
    ]
}

ignore_email_patterns = [
    '\( .*?ago \)',
    '"token": ".*?"',
    'expire in .*? .*?\.'
]


def email_args_to_filename(**email_args):    
    fn = "tests/{email_collection}_emails/{state}.html".format(**email_args)
    os.makedirs(os.path.dirname(fn), exist_ok=True)
    return fn

def get_reference_email(**email_args):
    #TODO: does it actually find it in CI?
    try:
        html = open(email_args_to_filename(**{**email_args, 'email_collection': 'reference'})).read() 
        return adapt_html(html, **email_args)
    except FileNotFoundError:
        return None

# substitute several patterns for comparison
def adapt_html(html, **email_args):
    for arg, patterns in generalized_email_patterns.items():
        if email_args[arg] is not None:
            for pattern in patterns:
                html = re.sub(pattern, r"\g<1>" + email_args[arg] + r"\g<3>", html)    
            
    return html

# ignore patterns which we are too lazy to substiture
def ignore_html_patterns(html):
    for pattern in ignore_email_patterns:
        html = re.sub(pattern, "<IGNORES>", html, flags=re.DOTALL)

    return html


def store_email(email_html, **email_args):
    # example for viewing
    fn = email_args_to_filename(**{**email_args, 'email_collection': 'to_review'})
    with open(fn, "w") as f:
        f.write(email_html)     

    open("to_review_email.html", "w").write(ignore_html_patterns(email_html))

    return fn

    
def validate_email_content(
                   message_record, 
                   state: str,
                   dispatcher_job_state: DispatcherJobState,
                   time_request_str: str=None,
                   products_url=None,
                   ):

    reference_email = get_reference_email(state=state, time_request_str=time_request_str, products_url=products_url)
    
    assert message_record['mail_from'] == 'team@odahub.io'
    assert message_record['rcpt_tos'] == ['mtm@mtmco.net', 'team@odahub.io']

    msg = email.message_from_string(message_record['data'])    

    assert msg['Subject'] == f"[ODA][{state}] dummy first requested at {time_request_str} job_id: {dispatcher_job_state.job_id[:8]}"
    assert msg['From'] == 'team@odahub.io'
    assert msg['To'] == 'mtm@mtmco.net'
    assert msg['CC'] == ", ".join(['team@odahub.io'])
    assert msg['Reply-To'] == "contact@odahub.io"
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

            fn = store_email(content_text_html, state=state, time_request_str=time_request_str, products_url=products_url)

            if reference_email is not None:
                open("adapted_reference.html", "w").write(ignore_html_patterns(reference_email))
                assert ignore_html_patterns(reference_email) == ignore_html_patterns(content_text_html), f"please inspect {fn} and possibly copy it to {fn.replace('to_review', 'reference')}"


        if content_text is not None:
            assert re.search(f'Dear User', content_text, re.IGNORECASE)
            assert re.search(f'Kind Regards', content_text, re.IGNORECASE)

            with open("email.text", "w") as f:
                f.write(content_text)

            if products_url is not None:                
                assert products_url in content_text



def get_expected_products_url(dict_param):
    dict_param_complete = dict_param.copy()    
    dict_param_complete.pop("token", None)
    dict_param_complete.pop("session_id", None)
    dict_param_complete.pop("job_id", None)

    assert 'session_id' not in dict_param_complete
    assert 'job_id' not in dict_param_complete
    assert 'token' not in dict_param_complete

    for key, value in dict(dict_param_complete).items():
        if value is None:
            dict_param_complete.pop(key)

    return '%s?%s' % ('PRODUCTS_URL', urlencode(dict_param_complete))


def test_validation_job_id(dispatcher_live_fixture):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
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
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c)
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'

    # let's generate another valid token, just for a different user
    token_payload = {
        **default_token_payload,
        "sub":"mtm1@mtmco.net"
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    dict_param = dict(
        query_status="ready",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        job_id=dispatcher_job_state.job_id,
        session_id=dispatcher_job_state.session_id,
        token=encoded_token
    )


    # this should return status submitted, so email sent
    c = requests.get(server + "/run_analysis",
                     dict_param
                     )
    dict_param.pop('token')
    dict_param.pop('session_id')
    dict_param.pop('job_id')
    dict_param['query_status'] = 'new'
    wrong_job_id = u'%s' % (make_hash({**dict_param, "sub": "mtm1@mtmco.net"}))
    assert c.status_code == 403
    jdata = c.json()
    assert jdata["exit_status"]["debug_message"] == \
           f'The provided job_id={dispatcher_job_state.job_id} does not match with the ' \
           f'job_id={wrong_job_id} derived from the request parameters for your user account email'
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == "user not authorized to download the requested product"


@pytest.mark.parametrize("default_values", [True, False])
@pytest.mark.parametrize("time_original_request_none", [False])
#why is it None sometimes, and should we really send an email in this case?..
#@pytest.mark.parametrize("time_original_request_none", [True, False])
@pytest.mark.parametrize("request_cred", ['public', 'private'])
def test_email_run_analysis_callback(dispatcher_long_living_fixture, dispatcher_local_mail_server, default_values, request_cred, time_original_request_none):
    server = dispatcher_long_living_fixture
    
    DispatcherJobState.remove_scratch_folders()

    # remove all the current scratch folders
    dir_list = glob.glob('scratch_*')
    [shutil.rmtree(d) for d in dir_list]

    token_none = ( request_cred == 'public' )
        
    
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

    products_url = get_expected_products_url(dict_param)
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

    # this dones nothing special
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
    jdata = dispatcher_job_state.load_job_state_record('node_final', 'done')    
            
    if token_none:
        assert 'email_status' not in jdata

    elif time_original_request_none:
        assert 'email_status' in jdata

    elif default_values:        
        assert 'email_status' not in jdata

    else:
        assert jdata['email_status'] == 'email sent'

        # check the email in the email folders, and that the first one was produced
        dispatcher_job_state.assert_email(state="done")
        
        # check the email in the log files
        validate_email_content(
            dispatcher_local_mail_server.get_email_record(0),
            'submitted',
            #'done',
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
    
    jdata = dispatcher_job_state.load_job_state_record('node_failed', 'failed')

    if token_none:
        # email not supposed to be sent for public request
        assert 'email_status' not in jdata
    else:
        assert jdata['email_status'] == 'email sent'

        # check the email in the email folders, and that the first one was produced        
        if default_values or time_original_request_none:
            dispatcher_job_state.assert_email('failed', comment="expected one email in total, failed")
            dispatcher_local_mail_server.assert_email_number(2)
        else:
            dispatcher_job_state.assert_email('failed', comment="expected two emails in total, second failed")
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


@pytest.mark.not_safe_parallel
def test_email_submitted_same_job(dispatcher_live_fixture, dispatcher_local_mail_server):
    # remove all the current scratch folders
    dir_list = glob.glob('scratch_*')
    [shutil.rmtree(d) for d in dir_list]

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
    
    dispatcher_job_state.assert_email(state="submitted")
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
        dispatcher_job_state.assert_email(state="submitted", number=1)
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
    
    dispatcher_job_state.assert_email(state="submitted", number=2)
    dispatcher_local_mail_server.assert_email_number(2)
    

    # let the interval time pass again, so that a new email si sent
    time.sleep(5)
    c = requests.get(server + "/run_analysis",
                     dict_param
                     )

    assert c.status_code == 200
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert jdata['exit_status']['email_status'] == 'email sent'

    # check the email in the email folders, and that the first one was produced
    dispatcher_local_mail_server.assert_email_number(3)


@pytest.mark.not_safe_parallel
def test_email_submitted_multiple_requests(dispatcher_live_fixture, dispatcher_local_mail_server):
    # remove all the current scratch folders
    dir_list = glob.glob('scratch_*')
    for d in dir_list:
        shutil.rmtree(d)

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
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
    assert c.status_code == 200

    logger.info("response from run_analysis: %s", json.dumps(c.json(), indent=4))

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c)    
    
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert jdata['exit_status']['email_status'] == 'email sent'

    # check the email in the email folders, and that the first one was produced
    dispatcher_job_state.assert_email('submitted')

    # re-submit the same request (so that the same job_id will be generated) but as a different session,
    # in order to produce a sequence of submitted status
    # and verify not a sequence of submitted-status emails are generated
    # a sequence of clicks of the link provided with the email is simulated
    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        token=encoded_token
    )

    for i in range(5):
        c = requests.get(server + "/run_analysis",
                         dict_param
                         )

        assert c.status_code == 200
        jdata = c.json()
        assert jdata['exit_status']['job_status'] == 'submitted'
        assert 'email_status' not in jdata['exit_status']

    # jobs will be aliased
    dispatcher_job_state.assert_email('submitted')
    

    # let the interval time pass, so that a new email si sent
    time.sleep(5)
    c = requests.get(server + "/run_analysis",
                     dict_param
                     )

    assert c.status_code == 200
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert jdata['exit_status']['email_status'] == 'email sent'
    session_id = jdata['session_id']

    # check the email in the email folders, and that the first one was produced
    assert os.path.exists(f'scratch_sid_{session_id}_jid_{dispatcher_job_state.job_id}_aliased')
    list_email_files_last_request = glob.glob(f'scratch_sid_{session_id}_jid_{dispatcher_job_state.job_id}_aliased/email_history/email_submitted_*.email')
    assert len(list_email_files_last_request) == 1
    list_overall_email_files = glob.glob(f'scratch_sid_*_jid_{dispatcher_job_state.job_id}*/email_history/email_submitted_*.email')
    assert len(list_overall_email_files) == 2


@pytest.mark.not_safe_parallel
def test_email_done(dispatcher_live_fixture, dispatcher_local_mail_server):
    DispatcherJobState.remove_scratch_folders()
    
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    token_payload = {
        **default_token_payload,
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

    logger.info("response from run_analysis: %s", json.dumps(c.json(), indent=4))
    jdata = c.json()

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c)
    
    time_request = jdata['time_request']
    
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

    jdata = dispatcher_job_state.load_job_state_record('node_final', 'done')
    assert 'email_status' in jdata
    assert jdata['email_status'] == 'email sent'

    # a number of done call_backs, but none should trigger the email sending since this already happened
    for i in range(3):
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

        jdata = dispatcher_job_state.load_job_state_record('node_final', 'done')
        
        assert 'email_status' in jdata
        assert jdata['email_status'] == 'multiple completion email detected'

    # check the email in the email folders, and that the first one was produced

    dispatcher_job_state.assert_email("submitted")
    dispatcher_job_state.assert_email("done")
        

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


@pytest.mark.not_safe_parallel
def test_email_callback_after_run_analysis_subprocess_mail_server(dispatcher_live_fixture, dispatcher_local_mail_server_subprocess):
    # remove all the current scratch folders
    dir_list = glob.glob('scratch_*')
    [shutil.rmtree(d) for d in dir_list]

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
    list_email_files = glob.glob(email_history_folder_path + '/email_*.email')
    assert len(list_email_files) == 1


def test_email_very_long_request_url(dispatcher_long_living_fixture, dispatcher_local_mail_server):
    # emails generally can not contain lines longer than 999 characters.
    # different SMTP servers will deal with these differently: 
    #  * some will respond with error, 
    #  * some, apparently, automatically introduce new line 
    # 
    # The latter  may cause an issue if it is added in the middle of data, 
    # e.g. in some random place in json 
    # we need:
    #  * to detect this and be clear we can not send these long lines. they are not often usable as URLs anyway
    #  * compress long parameters, e.g. selected_catalog
    #  * request by shortcut (job_d): but it is clear that it is not generally possible to derive parameters from job_id
    #  * make this or some other kind of URL shortener

    server = dispatcher_long_living_fixture
    
    DispatcherJobState.remove_scratch_folders()

     # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "tem": 0
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    # set the time the request was initiated
    time_request = time.time()

    name_parameter_value = "01"*1000

    c = requests.get(server + "/run_analysis",
                     params=dict(
                         query_status="new",
                         query_type="Real",
                         instrument="empty-async",
                         product_type="numerical",
                         string_like_name=name_parameter_value,
                         token=encoded_token,
                         time_request=time_request
                     ))

    logger.info("response from run_analysis: %s", json.dumps(c.json(), indent=4))

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c)    

    jdata = c.json()
    assert jdata['exit_status']['email_status'] == 'email sent'

    dispatcher_job_state.assert_email("submitted")

    email_data = dispatcher_job_state.load_emails()[0]

    print(email_data)

    short_url = f'PRODUCTS_URL/dispatch-data/resolve-job-url?job_id={dispatcher_job_state.job_id}&session_id={dispatcher_job_state.session_id}'

    assert short_url in email_data

    url = short_url.replace('PRODUCTS_URL/dispatch-data', server)

    print("url", url)

    c = requests.get(url, allow_redirects=False)

    assert c.status_code == 302

    redirect_url = parse.urlparse(c.headers['Location'])
    print(redirect_url)
        
    # TODO: complete this
    # compressed = "z%3A" + base64.b64encode(zlib.compress(json.dumps(name_parameter_value).encode())).decode()
    # assert compressed in email_data

def test_email_compress_request_url():    
    from cdci_data_analysis.analysis.email_helper import compress_request_url_params

    url = "http://localhost:8000/?" + urlencode(dict(
        par_int=123,
        par_str="01"*10000,
    ))

    compressed_url = compress_request_url_params(url, consider_args=['par_str'])

    assert len(compressed_url) < 200
    assert len(url) > 10000

@pytest.mark.skip(reason="unused")
def test_adapt_line_length_api_code_one_long():
    from cdci_data_analysis.analysis.email_helper import adapt_line_length_api_code

    line_break = '\n'
    long_line_code = "01 "*310
    add_line_continuation = "\\"
    adapted = adapt_line_length_api_code(long_line_code, max_length=50, line_break=line_break, add_line_continuation=add_line_continuation)

    assert len(adapted.split(line_break))  == int((300*2)/50) + 2

    print("unadapted long_line_code:" + long_line_code)
    print("adapted:\n" + adapted)

    assert adapted.replace(line_break, '').replace(add_line_continuation, '') == long_line_code

@pytest.mark.skip(reason="unused")
def test_adapt_line_length_api_code_two_lines():
    from cdci_data_analysis.analysis.email_helper import adapt_line_length_api_code

    line_break = '\n'
    long_line_code = "01 " * 60 + "\n" + \
                     "01 " * 10 
    add_line_continuation = "\\"
    adapted = adapt_line_length_api_code(long_line_code, max_length=50, line_break=line_break, add_line_continuation=add_line_continuation)

    assert len(adapted.split(line_break))  == 2 + 1

    print("unadapted long_line_code:" + long_line_code)
    print("adapted:\n" + adapted)

    assert adapted.replace(line_break, '').replace(add_line_continuation, '') == long_line_code



def test_wrap_api_code():
    from cdci_data_analysis.analysis.email_helper import wrap_python_code

    c = wrap_python_code("""
a = 1

bla = x()

bla = "asdasdas adasda sdasdas dasdas asdasdas adasda sdasdas dasdas asdasdas adasda sdasdas dasdas"
    """)

    print(c)