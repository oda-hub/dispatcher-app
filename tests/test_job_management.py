import shutil
from urllib import parse

import pytest
import requests
import json
import html
import os
import re
import time
import jwt
import logging
import email
from urllib.parse import parse_qs, urlencode, urlparse
import glob

from collections import OrderedDict

from cdci_data_analysis.pytest_fixtures import DispatcherJobState, make_hash, ask
from cdci_data_analysis.analysis.email_helper import textify_email

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
    ],
    'job_id': [
        '(job_id: )(.*?)(<)'
    ]
}

ignore_email_patterns = [
    r'\( .*?ago \)',
    r'&#34;token&#34;:.*?,',
    r'expire in .*? .*?\.'
]


def email_args_to_filename(**email_args):    
    suffix = "-".join(email_args.get('variation_suffixes', []))

    if suffix != "":
        suffix = "-" + suffix

    fn = "tests/{email_collection}_emails/{state}{suffix}.html".format(suffix=suffix, **email_args)
    os.makedirs(os.path.dirname(fn), exist_ok=True)
    return fn

def get_reference_email(**email_args):
    # TODO: does it actually find it in CI?
    fn = os.path.abspath(email_args_to_filename(**{**email_args, 'email_collection': 'reference'}))
    try:
        html_content = open(fn).read()
        return adapt_html(html_content, **email_args)
    except FileNotFoundError:
        if email_args.get('require', False):
            raise
        else:
            return None

# substitute several patterns for comparison
def adapt_html(html_content, **email_args):
    for arg, patterns in generalized_email_patterns.items():
        if email_args[arg] is not None:
            for pattern in patterns:
                html_content = re.sub(pattern, r"\g<1>" + email_args[arg] + r"\g<3>", html_content)

    return html_content

# ignore patterns which we are too lazy to substiture
def ignore_html_patterns(html_content):
    for pattern in ignore_email_patterns:
        html_content = re.sub(pattern, "<IGNORES>", html_content, flags=re.DOTALL)

    return html_content


def store_email(email_html, **email_args):
    # example for viewing
    fn = email_args_to_filename(**{**email_args, 'email_collection': 'to_review'})
    with open(fn, "w") as f:
        f.write(email_html)     

    open("to_review_email.html", "w").write(ignore_html_patterns(email_html))

    return fn

def extract_api_code(text):
    r = re.search('<div.*?>(.*?)</div>', text, flags=re.DOTALL)
    if r:
        return textify_email(r.group(1))
    else:
        with open("no-api-code-problem.html", "w") as f:
            f.write(text)
        raise RuntimeError("no api code in the email!")

def extract_products_url(text):
    r = re.search('<a href="(.*?)">url</a>', text, flags=re.DOTALL)
    if r:
        return r.group(1)
    else:
        with open("no-url-problem.html", "w") as f:
            f.write(text)
        raise RuntimeError("no products url in the email!")


def validate_api_code(api_code, dispatcher_live_fixture):
    if dispatcher_live_fixture is not None:
        api_code = api_code.replace("<br>", "")
        api_code = api_code.replace("PRODUCTS_URL/dispatch-data", dispatcher_live_fixture)

        my_globals = {}
        exec(api_code, my_globals)

        assert my_globals['data_collection']
        
        my_globals['data_collection'].show()


def validate_products_url(url, dispatcher_live_fixture):
    if dispatcher_live_fixture is not None:
        # this is URL to frontend; it's not really true that it is passed the same way to dispatcher in all cases
        # in particular, catalog seems to be passed differently!
        url = url.replace("PRODUCTS_URL", dispatcher_live_fixture + "/run_analysis")

        r = requests.get(url)

        assert r.status_code == 200

        jdata = r.json()

        assert jdata['exit_status']['status'] == 0
        assert jdata['exit_status']['job_status'] == 'done'
        
    
def validate_email_content(
                   message_record, 
                   state: str,
                   dispatcher_job_state: DispatcherJobState,
                   time_request_str: str=None,
                   products_url=None,
                   dispatcher_live_fixture=None,
                   request_params: dict=None,
                   expect_api_code=True,
                   variation_suffixes=None,
                   require_reference_email=False
                   ):

    if variation_suffixes is None:
        variation_suffixes = []

    if not expect_api_code:
        variation_suffixes.append("no-api-code")

    reference_email = get_reference_email(state=state, 
                                          time_request_str=time_request_str, 
                                          products_url=products_url, 
                                          job_id=dispatcher_job_state.job_id[:8],
                                          variation_suffixes=variation_suffixes,
                                          require=require_reference_email
                                          )

    if request_params is None:
        request_params = {}
    
    product = request_params.get('product_type', 'dummy')
    
    assert message_record['mail_from'] == 'team@odahub.io'
    assert message_record['rcpt_tos'] == ['mtm@mtmco.net', 'team@odahub.io', 'teamBcc@odahub.io']

    msg = email.message_from_string(message_record['data'])

    assert msg['Subject'] == f"[ODA][{state}] {product} first requested at {time_request_str} job_id: {dispatcher_job_state.job_id[:8]}"
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
                if products_url != "":
                    assert re.search(f'<a href="(.*)">.*?</a>', content_text_html, re.M).group(1) == products_url
                else:
                    assert re.search(f'<a href="(.*)">url</a>', content_text_html, re.M) == None

            fn = store_email(content_text_html, 
                             state=state, 
                             time_request_str=time_request_str, 
                             products_url=products_url, 
                             variation_suffixes=variation_suffixes)

            if reference_email is not None:
                open("adapted_reference.html", "w").write(ignore_html_patterns(reference_email))
                assert ignore_html_patterns(reference_email) == ignore_html_patterns(content_text_html), f"please inspect {fn} and possibly copy it to {fn.replace('to_review', 'reference')}"

            if expect_api_code:
                validate_api_code(
                    extract_api_code(content_text_html),
                    dispatcher_live_fixture
                )
            else:
                open("content.txt", "w").write(content_text)
                assert "Please note that we were not able to embed API code in this email" in content_text

            if products_url != "":
                validate_products_url(
                    extract_products_url(content_text_html),
                    dispatcher_live_fixture
                )

        if content_text is not None:
            assert re.search(f'Dear User', content_text, re.IGNORECASE)
            assert re.search(f'Kind Regards', content_text, re.IGNORECASE)

            with open("email.text", "w") as f:
                f.write(content_text)

            if products_url is not None and products_url != "":
                assert products_url in content_text


def get_expected_products_url(dict_param,
                              token,
                              session_id,
                              job_id):
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

    dict_param_complete = OrderedDict({
        k: dict_param_complete[k] for k in sorted(dict_param_complete.keys())
    })

    products_url = '%s?%s' % ('PRODUCTS_URL', urlencode(dict_param_complete))

    if len(products_url) > 2000:
        possibly_compressed_request_url = ""
    elif 2000 > len(products_url) > 600:
        possibly_compressed_request_url = \
            "PRODUCTS_URL/dispatch-data/resolve-job-url?" + \
            parse.urlencode(dict(job_id=job_id, session_id=session_id, token=token))
    else:
        possibly_compressed_request_url = products_url

    return possibly_compressed_request_url


def test_validation_job_id(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    DispatcherJobState.remove_scratch_folders()

    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    # these parameters define request content
    base_dict_param = dict(
        instrument="empty-async",
        product_type="dummy",
        query_type="real",
    )

    dict_param = dict(
        query_status="new",
        token=encoded_token,
        **base_dict_param
    )

    # this should return status submitted, so email sent
    c = requests.get(server + "/run_analysis",
                     dict_param
                     )

    print(json.dumps(c.json(), sort_keys=True, indent=4))

    assert c.status_code == 200
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'

    # let's generate another valid token, just for a different user
    token_payload['sub'] = "mtm1@mtmco.net"
        
    # this should return status submitted, so email sent    
    dict_param['token'] = jwt.encode(token_payload, secret_key, algorithm='HS256')
    dict_param['job_id'] = dispatcher_job_state.job_id # this is job id from different user
    dict_param['query_status'] = 'submitted'
    
    c = requests.get(server + "/run_analysis",
                     dict_param
                     )
    
    wrong_job_id = make_hash({**base_dict_param, "sub": "mtm1@mtmco.net"})

    from cdci_data_analysis.flask_app.dispatcher_query import InstrumentQueryBackEnd
    assert InstrumentQueryBackEnd.restricted_par_dic(dict_param) == base_dict_param

    assert c.status_code == 403, json.dumps(c.json(), indent=4, sort_keys=True)
    jdata = c.json()
    
    assert jdata["exit_status"]["debug_message"] == \
           f'The provided job_id={dispatcher_job_state.job_id} does not match with the ' \
           f'job_id={wrong_job_id} derived from the request parameters for your user account email; parameters are derived from recorded job state'
    assert jdata["exit_status"]["error_message"] == ""
    assert jdata["exit_status"]["message"] == "Request not authorized"


@pytest.mark.parametrize("default_values", [True, False])
@pytest.mark.parametrize("time_original_request_none", [False])
#why is it None sometimes, and should we really send an email in this case?..
#@pytest.mark.parametrize("time_original_request_none", [True, False])
@pytest.mark.parametrize("request_cred", ['public', 'private', 'private-no-email'])
def test_email_run_analysis_callback(dispatcher_long_living_fixture, dispatcher_local_mail_server, default_values, request_cred, time_original_request_none):
    from cdci_data_analysis.plugins.dummy_instrument.data_server_dispatcher import DataServerQuery
    DataServerQuery.set_status('submitted')

    server = dispatcher_long_living_fixture
    
    DispatcherJobState.remove_scratch_folders()
    
    token_none = ( request_cred == 'public' )

    expect_email = True

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

        if request_cred == 'private-no-email':
            token_payload['mssub'] = False
            token_payload['msdone'] = False
            token_payload['msfail'] = False
            expect_email = False            

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

    session_id = jdata['session_id']
    job_id = jdata['job_monitor']['job_id']

    products_url = get_expected_products_url({** dict_param, 'use_scws':'no'}, token=encoded_token, session_id=session_id, job_id=job_id)
    assert jdata['exit_status']['job_status'] == 'submitted'
    # get the original time the request was made
    assert 'time_request' in jdata
    # set the time the request was initiated
    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))
    
    if token_none or not expect_email:
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
            dispatcher_live_fixture=None,
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

    DataServerQuery.set_status('done')

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
            
    if token_none or not expect_email:
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
            dispatcher_local_mail_server.get_email_record(1),
            'done',
            dispatcher_job_state,
            time_request_str=time_request_str,
            dispatcher_live_fixture=server,
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

    if token_none or not expect_email:
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
            dispatcher_live_fixture=server,
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

    DataServerQuery.set_status('submitted') # sets the expected default for other tests


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
        "intsub": 5
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
    
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())
    
    #dict_param_complete = dict_param.copy()
    #dict_param_complete.pop("token")


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
def test_email_unnecessary_job_id(dispatcher_live_fixture, dispatcher_local_mail_server):
    # remove all the current scratch folders
    DispatcherJobState.remove_scratch_folders()

    server = dispatcher_live_fixture

    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        job_id="something-else"
    )

    # this should return status submitted, so email sent
    c = requests.get(server + "/run_analysis",
                     dict_param
                     )

    assert c.status_code == 400
        
    jdata = c.json()
    assert 'unnecessarily' in jdata['error'] 
    assert dict_param['job_id'] in jdata['error'] 
    

@pytest.mark.not_safe_parallel
def test_email_submitted_frontend_like_job_id(dispatcher_live_fixture, dispatcher_local_mail_server):
    DispatcherJobState.remove_scratch_folders()

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # email content in plain text and html format
    smtp_server_log = dispatcher_local_mail_server.local_smtp_output_json_fn

    encoded_token = jwt.encode(default_token_payload, secret_key, algorithm='HS256')

    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        token=encoded_token,
        job_id=""
    )

    # this should return status submitted, so email sent
    c = requests.get(server + "/run_analysis",
                     dict_param
                     )

    assert c.status_code == 200
    
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())
    
    
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert jdata['exit_status']['email_status'] == 'email sent'

    # check the email in the email folders, and that the first one was produced
    
    dispatcher_job_state.assert_email(state="submitted")
    dispatcher_local_mail_server.assert_email_number(1)


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
        "intsub": 5
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

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())
    
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
        print("i: ", i)
        assert jdata['exit_status']['job_status'] == 'submitted'
        assert 'email_status' not in jdata['exit_status']

    # jobs will be aliased
    dispatcher_job_state.assert_email('submitted')
    

    # let the interval time pass, so that a new email is sent
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

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())
    
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

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

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


@pytest.mark.parametrize("request_length", [600, 1000])
def test_email_very_long_request_url(dispatcher_long_living_fixture, dispatcher_local_mail_server, request_length):
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

    name_parameter_value = "01"*request_length

    dict_param = dict(
         query_status="new",
         query_type="Real",
         instrument="empty-async",
         product_type="numerical",
         string_like_name=name_parameter_value,
         token=encoded_token,
         time_request=time_request
    )

    c = requests.get(server + "/run_analysis",
                     params=dict_param)

    logger.info("response from run_analysis: %s", json.dumps(c.json(), indent=4))

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

    jdata = c.json()
    assert jdata['exit_status']['email_status'] == 'email sent'

    dispatcher_job_state.assert_email("submitted")

    email_data = dispatcher_job_state.load_emails()[0]

    print(email_data)

    session_id = jdata['session_id']
    job_id = jdata['job_monitor']['job_id']

    short_url = get_expected_products_url(dict_param, token=encoded_token, session_id=session_id, job_id=job_id)

    if short_url != "":
        assert short_url in email_data
        url = short_url.replace('PRODUCTS_URL/dispatch-data', server)

        print("url", url)

        c = requests.get(url, allow_redirects=False)

        assert c.status_code == 302, json.dumps(c.json(), sort_keys=True, indent=4)

        redirect_url = parse.urlparse(c.headers['Location'])
        print(redirect_url)

        # TODO: complete this
        # compressed = "z%3A" + base64.b64encode(zlib.compress(json.dumps(name_parameter_value).encode())).decode()
        # assert compressed in email_data
    else:
        assert """You can retrieve the results by repeating the request.
Unfortunately, due to a known issue with very large requests, a URL with the selected request parameters could not be generated.
This might be fixed in a future release.""" in email_data


email_scw_list_test_data = [
    # passing a list
    (True, 'form_list', 'list'),
    (True, 'form_list', 'string'),
    (True, 'user_file', 'list'),
    (True, 'user_file', 'string'),
    (True, 'no', 'list'),
    (True, 'no', 'string'),
    (True, None, 'list'),
    (True, None, 'string'),
    (True, 'not_included', 'list'),
    (True, 'not_included', 'string'),
    # not passing any list
    (False, 'form_list', None),
    (False, 'user_file', None),
    (False, 'no', None),
    (False, None, None),
    (False, 'not_included', None),
]


@pytest.mark.not_safe_parallel
@pytest.mark.parametrize("passing_scw_list, use_scws_value, scw_list_format", email_scw_list_test_data)
def test_email_scws_list(dispatcher_live_fixture,
                         dispatcher_local_mail_server,
                         passing_scw_list,
                         use_scws_value,
                         scw_list_format):
    DispatcherJobState.remove_scratch_folders()

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "unige-hpc-full, general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    # setting params
    params = {
        'query_status': "new",
        'product_type': "dummy",
        'query_type': "Real",
        'instrument': 'empty-async',
        'token': encoded_token
    }
    if use_scws_value != 'not_included':
        params['use_scws'] = use_scws_value

    check_email = True
    if use_scws_value == 'user_file':
        scw_list_file_obj = None
        if passing_scw_list:
            file_path = DispatcherJobState.create_scw_list_file(list_length=5,
                                                                string_format=(scw_list_format == 'string'))
            scw_list_file = open(file_path).read()
            scw_list_file_obj = {"user_scw_list_file": scw_list_file}
        # we are supposed to send a file, so it has to be a post request
        jdata = ask(server,
                    params,
                    method='post',
                    max_time_s=150,
                    expected_query_status=None,
                    expected_status_code=None,
                    files=scw_list_file_obj
                    )
        params['use_scws'] = 'form_list'
        if not passing_scw_list:
            assert jdata['error_message'] == ('Error while uploading scw_list file from the frontend: '
                                              'the file has not been provided')
            check_email = False
        else:
            assert 'scw_list' in jdata['products']['analysis_parameters']
    elif use_scws_value == 'form_list':
        if passing_scw_list:
            scw_list = [f"0665{i:04d}0010.001" for i in range(5)]
            params['scw_list'] = scw_list
            if scw_list_format == 'string':
                params['scw_list'] = ",".join(scw_list)
        jdata = ask(server,
                    params,
                    max_time_s=150,
                    expected_query_status=None,
                    expected_status_code=None
                    )
        if not passing_scw_list:
            assert jdata['error_message'] == (
                'scw_list parameter was expected to be passed, but it has not been found, '
                'please check the inputs you provided')
            check_email = False
        else:
            assert 'scw_list' in jdata['products']['analysis_parameters']
        params['use_scws'] = 'form_list'
    elif use_scws_value == 'no':
        # no list should be passed, but in case something is passed
        if passing_scw_list:
            scw_list = [f"0665{i:04d}0010.001" for i in range(5)]
            params['scw_list'] = scw_list
            if scw_list_format == 'string':
                params['scw_list'] = ",".join(scw_list)
        jdata = ask(server,
                    params,
                    max_time_s=150,
                    expected_query_status=None,
                    expected_status_code=None
                    )
        if passing_scw_list:
            # not allowed
            assert jdata['error_message'] == ("scw_list parameter was provided "
                                           "despite use_scws was indicating this was not provided, "
                                           "please check the inputs you provided")
            check_email = False
        params['use_scws'] = 'no'
    elif use_scws_value is None or use_scws_value == 'not_included':
        if passing_scw_list and scw_list_format is not None:
            scw_list = [f"0665{i:04d}0010.001" for i in range(5)]
            params['scw_list'] = scw_list
            if scw_list_format == 'string':
                params['scw_list'] = ",".join(scw_list)
        jdata = ask(server,
                    params,
                    max_time_s=150,
                    expected_query_status=None,
                    expected_status_code=None
                    )
        if passing_scw_list:
            params['use_scws'] = 'form_list'
        else:
            params['use_scws'] = 'no'

    if check_email:
        assert jdata['exit_status']['email_status'] == 'email sent'

        if passing_scw_list:
            params['scw_list'] = ",".join([f"0665{i:04d}0010.001" for i in range(5)])
            assert 'scw_list' in jdata['products']['api_code']

        assert 'use_scws' not in jdata['products']['analysis_parameters']
        assert 'use_scws' not in jdata['products']['api_code']
        # validate email content,
        dispatcher_job_state = DispatcherJobState.from_run_analysis_response(jdata)
        products_url = get_expected_products_url(params,
                                                 token=encoded_token,
                                                 session_id=dispatcher_job_state.session_id,
                                                 job_id=dispatcher_job_state.job_id)

        # extract api_code and url from the email
        msg = email.message_from_string(dispatcher_local_mail_server.get_email_record()['data'])
        for part in msg.walk():
            if part.get_content_type() == 'text/html':
                content_text_html = part.get_payload().replace('\r', '').strip()
                email_api_code = extract_api_code(content_text_html)
                assert 'use_scws' not in email_api_code
                if passing_scw_list:
                    assert 'scw_list' in email_api_code

                extracted_product_url = extract_products_url(content_text_html)
                if products_url is not None and products_url != "":
                    assert products_url == extracted_product_url

                # verify product url contains the use_scws parameter for the frontend
                extracted_parsed = parse.urlparse(extracted_product_url)
                assert 'use_scws' in parse_qs(extracted_parsed.query)


def test_email_parameters_html_conflicting(dispatcher_long_living_fixture, dispatcher_local_mail_server):
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

    name_parameter_value = "< bla bla: this is not a tag > <"

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

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

    jdata = c.json()
    assert jdata['exit_status']['email_status'] == 'email sent'

    dispatcher_job_state.assert_email("submitted")

    email_data = dispatcher_job_state.load_emails()[0]

    print(email_data)

    assert name_parameter_value in email_data

    from bs4 import BeautifulSoup
    assert name_parameter_value in BeautifulSoup(email_data).get_text()

@pytest.mark.parametrize('length', [3, 100])
def test_email_very_long_unbreakable_string(length, dispatcher_long_living_fixture, dispatcher_local_mail_server):
    unbreakable = length >= 100 

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
    

    params = dict(
            query_status="new",
            query_type="Real",
            instrument="empty-async",
            product_type="numerical",
            token=encoded_token,
        )

    # this kind of parameters never really happen, and we should be alerted
    # we might as well send something in email, like failed case. but better let's make us look immediately
    params['very_long_parameter_'*length] = "unset"

    c = requests.get(server + "/run_analysis",
                     params=params)

    logger.info("response from run_analysis: %s", json.dumps(c.json(), indent=4))

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

    jdata = c.json()

    assert jdata['exit_status']['email_status'] == 'email sent'
    products_url = get_expected_products_url(params, 
                                             token=encoded_token, 
                                             session_id=dispatcher_job_state.session_id, 
                                             job_id=dispatcher_job_state.job_id)
    assert jdata['exit_status']['job_status'] == 'submitted'
    # get the original time the request was made
    assert 'time_request' in jdata
    # set the time the request was initiated
    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))
    

    validate_email_content(
        dispatcher_local_mail_server.get_email_record(),
        'submitted',
        dispatcher_job_state,
        time_request_str=time_request_str,
        products_url=products_url,
        dispatcher_live_fixture=None,
        request_params=params,
        expect_api_code=not unbreakable,
        variation_suffixes=["numeric-not-very-long"] if not unbreakable else [],
        require_reference_email=True
    )
    
    # capture and verify sentry alert




def test_email_compress_request_url():    
    from cdci_data_analysis.analysis.email_helper import compress_request_url_params

    url = "http://localhost:8000/?" + urlencode(dict(
        par_int=123,
        par_str="01"*10000,
    ))

    compressed_url = compress_request_url_params(url, consider_args=['par_str'])

    assert len(compressed_url) < 200
    assert len(url) > 10000


def test_wrap_api_code():
    from cdci_data_analysis.analysis.email_helper import wrap_python_code

    max_length=50

    code = """
a = 1

def x(arg):
    return arg

bla = x("x")

bla = "asdasdas adasda sdasdas dasdas asdasdas adasda sdasdas dasdas asdasdas adasda sdasdas dasdas"

bla_bla = 'asdasdas adasda sdasdas dasdas asdasdas adasda sdasdas dasdas asdasdas adasda sdasdas dasdas asdasdas adasda sdasdas dasdas asdasdas adasda sdasdas dasdas asdasdas adasda sdasdas dasdas'

scwl_dict = {"scw_list": "115000860010.001,115000870010.001,115000980010.001,115000990010.001,115001000010.001,115001010010.001,115001020010.001,115001030010.001,115001040010.001,115001050010.001,115001060010.001,117100210010.001,118100040010.001,118100050010.001,118900100010.001,118900120010.001,118900130010.001,118900140010.001,119000020010.001,119000030010.001,119000040010.001,119000050010.001,119000190010.001,119900370010.001,119900480010.001,119900490010.001,119900500010.001,119900510010.001,119900520010.001,119900530010.001,119900540010.001,119900550010.001,119900560010.001,119900570010.001,119900670010.001,119900680010.001,119900690010.001,119900700010.001,119900710010.001,119900720010.001,119900730010.001,119900740010.001,119900750010.001,119900760010.001,119900770010.001,119900880010.001,119900890010.001,119900900010.001,119900910010."}
    """
    
    c = wrap_python_code(code, max_length=max_length)

    print("wrapped:\n", c)

    assert max([ len(l) for l in c.split("\n") ]) < max_length

    my_globals = {}
    exec(c, my_globals)

    assert len(my_globals['bla']) > max_length
    assert len(my_globals['scwl_dict']['scw_list']) > max_length

    
    