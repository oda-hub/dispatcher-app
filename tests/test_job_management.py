import shutil
from urllib import parse

import pytest
import requests
import json
import os
import re
import time
import jwt
import logging
import email
from urllib.parse import parse_qs, urlencode
import glob

from cdci_data_analysis.analysis.catalog import BasicCatalog
from cdci_data_analysis.pytest_fixtures import DispatcherJobState, make_hash, ask
from cdci_data_analysis.plugins.dummy_plugin.data_server_dispatcher import DataServerQuery

from datetime import datetime

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

    DataServerQuery.set_status('submitted')

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


def email_attachment_args_to_filename(**email_attachment_args):
    fn = "tests/{email_collection}_emails/{name}_{state}".format(**email_attachment_args)
    return fn


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


def get_incident_report_reference_email(**email_args):
    fn = os.path.join('reference_emails', 'incident_report.html')

    try:
        html_content = open(fn).read()
        return adapt_html(html_content, patterns=DispatcherJobState.generalized_incident_patterns, **email_args)
    except FileNotFoundError:
        return None


def get_reference_attachment(**email_attachment_args):
    fn = os.path.abspath(email_attachment_args_to_filename(**{**email_attachment_args, 'email_collection': 'reference'}))
    try:
        attachment_content = open(fn).read()
        return attachment_content
    except FileNotFoundError:
            raise


# substitute several patterns for comparison
def adapt_html(html_content, patterns=None, **email_args,):
    if patterns is None:
        patterns = DispatcherJobState.generalized_patterns
    for arg, patterns in patterns.items():
        if arg in email_args and email_args[arg] is not None:
            for pattern in patterns:
                html_content = re.sub(pattern, r"\g<1>" + email_args[arg] + r"\g<3>", html_content)

    return html_content


# ignore patterns which we are too lazy to substitute
def apply_ignore_api_code_patterns(api_code_content):
    for pattern in DispatcherJobState.api_attachment_ignore_attachment_patterns:
        api_code_content = re.sub(pattern, "<IGNORES>", api_code_content, flags=re.DOTALL)

    return api_code_content


def store_email(email_html, **email_args):
    # example for viewing
    fn = email_args_to_filename(**{**email_args, 'email_collection': 'to_review'})
    with open(fn, "w") as f:
        f.write(email_html)     

    open("to_review_email.html", "w").write(DispatcherJobState.ignore_html_patterns(email_html))

    return fn


def validate_scw_list_email_content(message_record,
                                    scw_list,
                                    request_params=None,
                                    scw_list_passage='not_passed',
                                    products_url=None,
                                    dispatcher_live_fixture=None):
    scw_list_string = ",".join(scw_list)
    msg = email.message_from_string(message_record['data'])
    for part in msg.walk():
        if part.get_content_type() == 'text/html':
            content_text_html = part.get_payload().replace('\r', '').strip()
            email_api_code = DispatcherJobState.extract_api_code_from_text(content_text_html)
            assert 'use_scws' not in email_api_code

            if scw_list_passage != 'not_passed':
                assert 'scw_list' in email_api_code

            extracted_product_url = DispatcherJobState.extract_products_url(content_text_html)
            if products_url is not None and products_url != "":
                assert products_url == extracted_product_url

            if 'resolve' in extracted_product_url:
                print("need to resolve this:", extracted_product_url)
                extracted_product_url = DispatcherJobState.validate_resolve_url(extracted_product_url, dispatcher_live_fixture)

            # verify product url contains the use_scws parameter for the frontend
            extracted_parsed = parse.urlparse(extracted_product_url)
            assert 'use_scws' in parse_qs(extracted_parsed.query)
            extracted_use_scws = parse_qs(extracted_parsed.query)['use_scws'][0]
            assert extracted_use_scws == request_params['use_scws']
            if scw_list_passage != 'not_passed':
                assert 'scw_list' in parse_qs(extracted_parsed.query)
                extracted_scw_list = parse_qs(extracted_parsed.query)['scw_list'][0]
                assert extracted_scw_list == scw_list_string


def validate_catalog_email_content(message_record,
                                   products_url=None,
                                   dispatcher_live_fixture=None
                                   ):
    msg = email.message_from_string(message_record['data'])
    for part in msg.walk():
        if part.get_content_type() == 'text/html':
            content_text_html = part.get_payload().replace('\r', '').strip()
            email_api_code = DispatcherJobState.extract_api_code_from_text(content_text_html)
            assert 'selected_catalog' in email_api_code

            extracted_product_url = DispatcherJobState.extract_products_url(content_text_html)
            if products_url is not None:
                assert products_url == extracted_product_url

            if 'resolve' in extracted_product_url:
                print("need to resolve this:", extracted_product_url)
                extracted_product_url = DispatcherJobState.validate_resolve_url(extracted_product_url, dispatcher_live_fixture)

            if extracted_product_url is not None and extracted_product_url != '':
                extracted_parsed = parse.urlparse(extracted_product_url)
                assert 'selected_catalog' in parse_qs(extracted_parsed.query)


def validate_email_content(
                   message_record, 
                   state: str,
                   dispatcher_job_state: DispatcherJobState,
                   time_request_str: str=None,
                   products_url=None,
                   dispatcher_live_fixture=None,
                   request_params: dict=None,
                   expect_api_code=True,
                   expect_api_code_attachment=False,
                   variation_suffixes=None,
                   require_reference_email=False,
                   state_title=None
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
    reference_api_code_attachment = None
    if expect_api_code_attachment:
        reference_api_code_attachment = get_reference_attachment(state=state, name="api_code_attachment")

    if request_params is None:
        request_params = {}
    
    product = request_params.get('product_type', 'dummy')
    
    assert message_record['mail_from'] == 'team@odahub.io'
    assert message_record['rcpt_tos'] == ['mtm@mtmco.net', 'team@odahub.io', 'teamBcc@odahub.io']

    msg = email.message_from_string(message_record['data'])

    if state_title is None:
        state_title = state

    assert msg['Subject'] == f"[ODA][{state_title}] {product} requested at {time_request_str} job_id: {dispatcher_job_state.job_id[:8]}"
    assert msg['From'] == 'team@odahub.io'
    assert msg['To'] == 'mtm@mtmco.net'
    assert msg['CC'] == ", ".join(['team@odahub.io'])
    assert msg['Reply-To'] == "contact@odahub.io"
    assert msg.is_multipart()
    
    for part in msg.walk():
        content_text = None
        content_disposition = str(part.get("Content-Disposition"))
        content_type = part.get_content_type()

        if "attachment" in content_disposition:
            # extract the payload
            if expect_api_code_attachment:
                assert part.get_filename() == 'api_code.py'
                attachment_api_code = part.get_payload(decode=True).decode()
                if reference_api_code_attachment is not None:
                    assert apply_ignore_api_code_patterns(reference_api_code_attachment) == apply_ignore_api_code_patterns(attachment_api_code)

        if content_type == 'text/plain':
            content_text_plain = part.get_payload().replace('\r', '').strip()
            content_text = content_text_plain
        elif content_type == 'text/html':
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
                open("adapted_reference.html", "w").write(DispatcherJobState.ignore_html_patterns(reference_email))
                assert DispatcherJobState.ignore_html_patterns(reference_email) == DispatcherJobState.ignore_html_patterns(content_text_html), f"please inspect {fn} and possibly copy it to {fn.replace('to_review', 'reference')}"

            if expect_api_code:
                DispatcherJobState.validate_api_code(
                    DispatcherJobState.extract_api_code_from_text(content_text_html),
                    dispatcher_live_fixture,
                    product_type=product
                )
            else:
                open("content.txt", "w").write(content_text)
                assert "Please note the API code for this query was too large to embed it in the email text. Instead," \
                       " we attach it as a python script." in content_text

            if products_url != "":
                DispatcherJobState.validate_products_url(
                    DispatcherJobState.extract_products_url(content_text_html),
                    dispatcher_live_fixture,
                    product_type=product
                )

        if content_text is not None:
            assert re.search(f'Dear User', content_text, re.IGNORECASE)
            assert re.search(f'Kind Regards', content_text, re.IGNORECASE)

            with open("email.text", "w") as f:
                f.write(content_text)

            if products_url is not None and products_url != "":
                assert products_url in content_text


def validate_incident_email_content(
        message_record,
        dispatcher_test_conf,
        dispatcher_job_state: DispatcherJobState,
        incident_time_str: str = None,
        incident_report_str: str = None,
        decoded_token = None
):

    assert message_record['mail_from'] == dispatcher_test_conf['email_options']['incident_report_email_options']['incident_report_sender_email_address']

    msg = email.message_from_string(message_record['data'])

    assert msg['Subject'] == f"[ODA][Report] Incident at {incident_time_str} job_id: {dispatcher_job_state.job_id}"
    assert msg['From'] == dispatcher_test_conf['email_options']['incident_report_email_options']['incident_report_sender_email_address']
    assert msg['To'] == ", ".join(dispatcher_test_conf['email_options']['incident_report_email_options']['incident_report_receivers_email_addresses'])
    assert msg.is_multipart()

    user_email_address = ""
    if decoded_token is not None:
        user_email_address = decoded_token.get('sub', None)
    reference_email = get_incident_report_reference_email(incident_time=incident_time_str,
                                                          job_id=dispatcher_job_state.job_id,
                                                          session_id=dispatcher_job_state.session_id,
                                                          incident_report=incident_report_str,
                                                          user_email_address=user_email_address
                                                          )

    for part in msg.walk():
        content_text = None
        # content_disposition = str(part.get("Content-Disposition"))
        content_type = part.get_content_type()

        # TODO to update this check when attachments will be used
        # if "attachment" in content_disposition:
            # extract the payload
            # if attachment:

        if content_type == 'text/plain':
            content_text_plain = part.get_payload().replace('\r', '').strip()
            content_text = content_text_plain
            if content_text is not None:
                assert re.search('A new incident has been reported to the dispatcher. More information can ben found below.', content_text, re.IGNORECASE)
                assert re.search('Execution details', content_text, re.IGNORECASE)
                assert re.search('Incident details', content_text, re.IGNORECASE)
                assert re.search(f'job_id: {dispatcher_job_state.job_id}', content_text, re.IGNORECASE)
                assert re.search(f'session_id: {dispatcher_job_state.session_id}', content_text, re.IGNORECASE)
                if decoded_token is not None:
                    assert re.search(f'user email address: {decoded_token["sub"]}', content_text, re.IGNORECASE)
                if incident_report_str is not None:
                    assert re.search(incident_report_str, content_text, re.IGNORECASE)
        elif content_type == 'text/html':
            content_text_html = part.get_payload().replace('\r', '').strip()
            content_text = content_text_html

            if reference_email is not None:
                open("adapted_incident_reference.html", "w").write(DispatcherJobState.ignore_html_patterns(reference_email))
                assert DispatcherJobState.ignore_html_patterns(reference_email) == DispatcherJobState.ignore_html_patterns(content_text_html)


@pytest.mark.not_safe_parallel
def test_resubmission_job_id(dispatcher_live_fixture_no_resubmit_timeout):
    server = dispatcher_live_fixture_no_resubmit_timeout
    DispatcherJobState.remove_scratch_folders()
    DataServerQuery.set_status('')
    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    # these parameters define request content
    base_dict_param = dict(
        instrument="empty-async",
        product_type="dummy-log-submit",
        query_type="Real",
    )

    dict_param = dict(
        query_status="new",
        token=encoded_token,
        **base_dict_param
    )

    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    print(json.dumps(c.json(), sort_keys=True, indent=4))

    assert c.status_code == 200
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert DataServerQuery.get_status() == 'submitted'

    # resubmit the job before the timeout expires
    dict_param['job_id'] = dispatcher_job_state.job_id
    dict_param['query_status'] = 'submitted'
    DataServerQuery.set_status('')
    #
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    assert c.status_code == 200
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert DataServerQuery.get_status() == ''

    # resubmit the job after the timeout expired
    time.sleep(10.5)
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    assert c.status_code == 200
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert DataServerQuery.get_status() == 'submitted'

    # resubmit the job to get job ready
    DataServerQuery.set_status('done')

    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    assert c.status_code == 200
    jdata = c.json()
    assert jdata['exit_status']['job_status'] == 'ready'


def test_validation_job_id(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    DispatcherJobState.remove_scratch_folders()
    DataServerQuery.set_status('submitted')
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
    
    wrong_job_id = make_hash(
        {
            **base_dict_param,
            'sub': 'mtm1@mtmco.net',
            'src_name': '1E 1740.7-2942',
            'RA': 265.97845833,
            'DEC': -29.74516667,
            'T1': '2017-03-06T13:26:48.000',
            'T2': '2017-03-06T15:32:27.000',
            'T_format': 'isot'
        }
    )

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
# why is it None sometimes, and should we really send an email in this case?..
# @pytest.mark.parametrize("time_original_request_none", [True, False])
@pytest.mark.parametrize("request_cred", ['public', 'private', 'private-no-email'])
def test_email_run_analysis_callback(gunicorn_dispatcher_long_living_fixture, dispatcher_local_mail_server, default_values, request_cred, time_original_request_none):
    from cdci_data_analysis.plugins.dummy_plugin.data_server_dispatcher import DataServerQuery
    DataServerQuery.set_status('submitted')

    server = gunicorn_dispatcher_long_living_fixture

    DispatcherJobState.remove_scratch_folders()

    token_none = (request_cred == 'public')

    expect_email = True
    token_payload = {
            **default_token_payload,
            "tem": 0
        }

    if token_none:
        encoded_token = None
    else:
        # let's generate a valid token with high threshold

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
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )                     
    assert c.status_code == 200
    jdata = c.json()
    
    logger.info("response from run_analysis: %s", json.dumps(jdata, indent=4))
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

    assert jdata['query_status'] == "submitted"

    session_id = jdata['session_id']
    job_id = jdata['job_monitor']['job_id']

    completed_dict_param = {** dict_param,
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
    
    if token_none or not expect_email:
        # email not supposed to be sent for public request
        assert 'email_status' not in jdata
    else:
        assert jdata['exit_status']['email_status'] == 'email sent'
        
        validate_email_content(
            dispatcher_local_mail_server.get_email_record(),
            'submitted',
            dispatcher_job_state,
            variation_suffixes=["dummy"],
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
        assert dispatcher_job_state.load_job_state_record(f'node_{i}', "progressing")['full_report_dict']['action'] == current_action

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
        assert c.json()['query_status'] == 'progress' # always progress!

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
                        node_id=f'node_{i+1}',
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

    # TODO: test that this returns the result

    DataServerQuery.set_status('submitted') # sets the expected default for other tests

    r = requests.get(os.path.join(gunicorn_dispatcher_long_living_fixture, "inspect-state"), params=dict(token=encoded_token))
    assert r.status_code == 403
    if encoded_token is None:
        assert r.text == 'A token must be provided.'
    else:
        assert r.text == ("Unfortunately, your privileges are not sufficient to inspect the state for a given job_id.\n"
                          "Your privilege roles include ['general'], but the following roles are"
                          " missing: job manager.")

    admin_token = jwt.encode({**token_payload, 'roles': 'private, user manager, admin, job manager, administrator'}, secret_key, algorithm='HS256')
    r = requests.get(os.path.join(gunicorn_dispatcher_long_living_fixture, "inspect-state"), params=dict(token=admin_token))
    dispatcher_state_report = r.json()
    logger.info('dispatcher_state_report: %s', dispatcher_state_report)

    assert len(dispatcher_state_report['records']) > 0


@pytest.mark.not_safe_parallel
def test_email_submitted_faulty_time_request(dispatcher_live_fixture, dispatcher_local_mail_server):
    # remove all the current scratch folders
    dir_list = glob.glob('scratch_*')
    [shutil.rmtree(d) for d in dir_list]

    DataServerQuery.set_status('submitted')

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
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    assert c.status_code == 200

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

    jdata = c.json()
    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))
    assert jdata['exit_status']['job_status'] == 'submitted'
    assert jdata['exit_status']['email_status'] == 'email sent'

    # taken from the sentry log
    faulty_first_submitted_email_time = 325666656000000000

    list_email_files = glob.glob(os.path.join(dispatcher_job_state.email_history_folder, f'email_submitted_*.email'))
    assert len(list_email_files) == 1

    email_file_split_name, email_file_split_ext = os.path.splitext(os.path.basename(list_email_files[0]))
    email_file_split = email_file_split_name.split('_')
    assert float(email_file_split[3]) == time_request

    msg = dispatcher_local_mail_server.local_smtp_output[0]
    msg_data = email.message_from_string(msg['data'])
    assert msg_data[
               'Subject'] == f"[ODA][submitted] dummy requested at {time_request_str} job_id: {dispatcher_job_state.job_id[:8]}"

    email_file_split[3] = str(faulty_first_submitted_email_time)
    faulty_email_file_name = "_".join(email_file_split)

    os.rename(list_email_files[0], os.path.join(os.path.dirname(list_email_files[0]),faulty_email_file_name + email_file_split_ext))

    # let the interval time pass, so that a new email si sent
    time.sleep(5)
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

    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    assert c.status_code == 200
    jdata = c.json()

    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))

    assert jdata['exit_status']['job_status'] == 'submitted'
    assert jdata['exit_status']['email_status'] == 'email sent'

    list_email_files = glob.glob(os.path.join(dispatcher_job_state.email_history_folder, f'email_submitted_*.email'))
    assert len(list_email_files) == 2

    submitted_email_files = sorted(list_email_files, key=os.path.getmtime)

    f_name, f_ext = os.path.splitext(os.path.basename(submitted_email_files[-1]))
    f_name_splited = f_name.split('_')
    assert len(f_name_splited) == 4
    assert float(f_name.split('_')[3]) == time_request

    msg = dispatcher_local_mail_server.local_smtp_output[-1]
    msg_data = email.message_from_string(msg['data'])
    assert msg_data[
               'Subject'] == f"[ODA][submitted] dummy requested at {time_request_str} job_id: {dispatcher_job_state.job_id[:8]}"

    # let the interval time pass, so that a new email si sent
    time.sleep(5)

    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )

    assert c.status_code == 200
    jdata = c.json()

    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))

    assert jdata['exit_status']['job_status'] == 'submitted'
    assert jdata['exit_status']['email_status'] == 'email sent'

    list_email_files = glob.glob(os.path.join(dispatcher_job_state.email_history_folder, f'email_submitted_*.email'))
    assert len(list_email_files) == 3

    submitted_email_files = sorted(list_email_files, key=os.path.getmtime)

    f_name, f_ext = os.path.splitext(os.path.basename(submitted_email_files[-1]))
    f_name_splited = f_name.split('_')
    assert len(f_name_splited) == 4
    assert float(f_name.split('_')[3]) == time_request

    msg = dispatcher_local_mail_server.local_smtp_output[-1]
    msg_data = email.message_from_string(msg['data'])
    assert msg_data[
               'Subject'] == f"[ODA][submitted] dummy requested at {time_request_str} job_id: {dispatcher_job_state.job_id[:8]}"


@pytest.mark.not_safe_parallel
def test_email_submitted_same_job(dispatcher_live_fixture, dispatcher_local_mail_server):
    # remove all the current scratch folders
    dir_list = glob.glob('scratch_*')
    [shutil.rmtree(d) for d in dir_list]

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    DataServerQuery.set_status('submitted')

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

    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))

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

    # check the time in the title and filename is still the one of the first request
    for msg in dispatcher_local_mail_server.local_smtp_output:
        msg_data = email.message_from_string(msg['data'])
        assert msg_data[
                   'Subject'] == f"[ODA][submitted] dummy requested at {time_request_str} job_id: {dispatcher_job_state.job_id[:8]}"

    list_email_files = glob.glob(os.path.join(dispatcher_job_state.email_history_folder, f'email_submitted_*.email'))
    assert len(list_email_files) == 2
    for email_file in list_email_files:
        f_name, f_ext = os.path.splitext(os.path.basename(email_file))
        f_name_splited = f_name.split('_')
        assert len(f_name_splited) == 4
        assert float(f_name.split('_')[3]) == time_request

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

    # check the time in the title and filename is still the one of the first request
    for msg in dispatcher_local_mail_server.local_smtp_output:
        msg_data = email.message_from_string(msg['data'])
        assert msg_data[
                   'Subject'] == f"[ODA][submitted] dummy requested at {time_request_str} job_id: {dispatcher_job_state.job_id[:8]}"
    list_email_files = glob.glob(os.path.join(dispatcher_job_state.email_history_folder, f'email_submitted_*.email'))
    assert len(list_email_files) == 3
    for email_file in list_email_files:
        f_name, f_ext = os.path.splitext(os.path.basename(email_file))
        f_name_splited = f_name.split('_')
        assert len(f_name_splited) == 4
        assert float(f_name.split('_')[3]) == time_request


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

    DataServerQuery.set_status('submitted')

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

    DataServerQuery.set_status('submitted')

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
def test_email_done(gunicorn_dispatcher_live_fixture, dispatcher_local_mail_server):
    DispatcherJobState.remove_scratch_folders()
    DataServerQuery.set_status('submitted')
    
    server = gunicorn_dispatcher_live_fixture
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

    # check the email in the email folders, and that the first one was produced
    email_history_log_files = glob.glob(
        os.path.join(dispatcher_job_state.scratch_dir, 'email_history') + '/email_history_log_*.log')
    latest_file_email_history_log_file = max(email_history_log_files, key=os.path.getctime)
    with open(latest_file_email_history_log_file) as email_history_log_content_fn:
        history_log_content = json.loads(email_history_log_content_fn.read())
        logger.info("content email history logging: %s", history_log_content)
        assert history_log_content['job_id'] == dispatcher_job_state.job_id
        assert history_log_content['status'] == 'submitted'
        assert isinstance(history_log_content['additional_information']['submitted_email_files'], list)
        assert len(history_log_content['additional_information']['submitted_email_files']) == 0
        assert history_log_content['additional_information']['check_result_message'] == 'the email will be sent'
    
    time_request = jdata['time_request']

    DataServerQuery.set_status('done')

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

    # check the email in the email folders, and that the first one was produced
    email_history_log_files = glob.glob(
        os.path.join(dispatcher_job_state.scratch_dir, 'email_history') + '/email_history_log_*.log')
    latest_file_email_history_log_file = max(email_history_log_files, key=os.path.getctime)
    with open(latest_file_email_history_log_file) as email_history_log_content_fn:
        history_log_content = json.loads(email_history_log_content_fn.read())
        logger.info("content email history logging: %s", history_log_content)
        assert history_log_content['job_id'] == dispatcher_job_state.job_id
        assert history_log_content['status'] == 'done'
        assert isinstance(history_log_content['additional_information']['done_email_files'], list)
        assert len(history_log_content['additional_information']['done_email_files']) == 0
        assert history_log_content['additional_information']['check_result_message'] == 'the email will be sent'

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
        assert jdata['email_status'] == 'attempted repeated sending of completion email detected'

    # check the email in the email folders, and that the first one was produced

    dispatcher_job_state.assert_email("submitted")
    dispatcher_job_state.assert_email("done")

    email_history_log_files = glob.glob(
        os.path.join(dispatcher_job_state.scratch_dir, 'email_history') + '/email_history_log_*.log')
    latest_file_email_history_log_file = max(email_history_log_files, key=os.path.getctime)
    with open(latest_file_email_history_log_file) as email_history_log_content_fn:
        history_log_content = json.loads(email_history_log_content_fn.read())
        logger.info("content email history logging: %s", history_log_content)
        assert history_log_content['job_id'] == dispatcher_job_state.job_id
        assert history_log_content['status'] == 'done'
        assert isinstance(history_log_content['additional_information']['done_email_files'], list)
        assert len(history_log_content['additional_information']['done_email_files']) == 0
        assert history_log_content['additional_information']['check_result_message'] == 'the email will be sent'


@pytest.mark.not_safe_parallel
def test_status_details_email_done(gunicorn_dispatcher_live_fixture, dispatcher_local_mail_server):
    DispatcherJobState.remove_scratch_folders()

    server = gunicorn_dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    token_payload = {
        **default_token_payload,
        "tem": 0
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

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(jdata)

    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))

    DataServerQuery.set_status('done')

    c = requests.get(os.path.join(server, "call_back"),
                     params=dict(
                         job_id=dispatcher_job_state.job_id,
                         session_id=dispatcher_job_state.session_id,
                         # necessary to use the empty-async instrument otherwise a OsaJob would not be instantiated and
                         # TODO is the list of instruments for creating OsaJob objects within the job_factory complete?
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

    # check the additional status details within the email
    assert 'email_status_details' in jdata
    assert jdata['email_status_details'] == {
        'exception_message': 'Error when getting query products\nInstrument: empty, product: failing\n\n'
                             'The support team has been notified, and we are investigating to resolve the issue as soon as possible\n\n'
                             'If you are willing to help us, please use the "Write a feedback" button below. '
                             'We will make sure to respond to any feedback provided',
        'status': 'empty_product'
    }

    completed_dict_param = {**params,
                            'p_list': '[]',
                            'use_scws': 'no',
                            'src_name': '1E 1740.7-2942',
                            'RA': 265.97845833,
                            'DEC': -29.74516667,
                            'T1': '2017-03-06T13:26:48.000',
                            'T2': '2017-03-06T15:32:27.000',
                            'T_format': 'isot'
                            }

    products_url = DispatcherJobState.get_expected_products_url(completed_dict_param,
                                                                session_id=dispatcher_job_state.session_id,
                                                                job_id=dispatcher_job_state.job_id,
                                                                token=encoded_token)

    # check the email in the log files
    validate_email_content(
        dispatcher_local_mail_server.get_email_record(),
        'done',
        dispatcher_job_state,
        state_title='finished: with empty product',
        variation_suffixes=["failing"],
        time_request_str=time_request_str,
        products_url=products_url,
        request_params=params,
        dispatcher_live_fixture=server,
    )


def test_email_failure_callback_after_run_analysis(dispatcher_live_fixture, sentry_sdk_fixture):
    # TODO: for now, this is not very different from no-prior-run_analysis. This will improve

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
    DataServerQuery.set_status('submitted')
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

    email_history_log_files = glob.glob(os.path.join(dispatcher_job_state.scratch_dir, 'email_history') + '/email_history_log_*.log')
    latest_file_email_history_log_file = max(email_history_log_files, key=os.path.getctime)
    with open(latest_file_email_history_log_file) as email_history_log_content_fn:
        history_log_content = json.loads(email_history_log_content_fn.read())
        logger.info("content email history logging: %s", history_log_content)
        assert history_log_content['job_id'] == dispatcher_job_state.job_id
        assert history_log_content['status'] == 'failed'
        assert history_log_content['additional_information']['check_result_message'] == 'the email will be sent'


@pytest.mark.not_safe_parallel
def test_email_callback_after_run_analysis_subprocess_mail_server(dispatcher_live_fixture, dispatcher_local_mail_server_subprocess):
    # remove all the current scratch folders
    DispatcherJobState.remove_scratch_folders()

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
def test_email_very_long_request_url(dispatcher_long_living_fixture,
                                     dispatcher_local_mail_server,
                                     request_length):
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
    #  * request by shortcut (job_id): but it is clear that it is not generally possible to derive parameters from job_id
    #  * make this or some other kind of URL shortener

    server = dispatcher_long_living_fixture
    DataServerQuery.set_status('submitted')
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

    short_url = DispatcherJobState.get_expected_products_url(dict_param, session_id=session_id, job_id=job_id, token=encoded_token)

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


@pytest.mark.parametrize("expired_token", [True, False])
def test_email_link_job_resolution(dispatcher_long_living_fixture,
                                   dispatcher_local_mail_server,
                                   expired_token):

    server = dispatcher_long_living_fixture

    DispatcherJobState.remove_scratch_folders()
    DataServerQuery.set_status('submitted')

    # let's generate a valid token with high threshold
    token_payload = {
        **default_token_payload,
        "tem": 0
    }

    if expired_token:
        exp_time = int(time.time()) + 15
        token_payload["exp"] = exp_time

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    # based on the previous test
    name_parameter_value = "01" * 600

    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="numerical",
        string_like_name=name_parameter_value,
        token=encoded_token
    )

    jdata = ask(server,
                dict_param,
                expected_query_status=["submitted"],
                max_time_s=150,
                )

    logger.info("response from run_analysis: %s", json.dumps(jdata, indent=4))

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(jdata)

    assert jdata['exit_status']['email_status'] == 'email sent'

    dispatcher_job_state.assert_email("submitted")

    session_id = jdata['session_id']
    job_id = jdata['job_monitor']['job_id']

    expected_products_url = DispatcherJobState.get_expected_products_url(dict_param, session_id=session_id, token=encoded_token, job_id=job_id)

    if expired_token:
        # let make sure the token used for the previous request expires
        time.sleep(15)

    # set the time the request was initiated
    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))

    token_exp_time_str = datetime.fromtimestamp(float(token_payload["exp"])).strftime("%Y-%m-%d %H:%M:%S")

    reference_email = get_reference_email(state='submitted',
                                          time_request_str=time_request_str,
                                          products_url=expected_products_url,
                                          job_id=dispatcher_job_state.job_id[:8],
                                          variation_suffixes=["numeric-very-long-not-permanent"],
                                          require=True,
                                          token_exp_time_str=token_exp_time_str
                                          )

    # extract api_code and url from the email
    msg = email.message_from_string(dispatcher_local_mail_server.get_email_record()['data'])
    for part in msg.walk():
        if part.get_content_type() == 'text/html':
            content_text_html = part.get_payload().replace('\r', '').strip()

            extracted_product_url = DispatcherJobState.extract_products_url(content_text_html)
            assert expected_products_url == extracted_product_url

            fn = store_email(content_text_html,
                             state='submitted',
                             time_request_str=time_request_str,
                             products_url=expected_products_url,
                             variation_suffixes=["numeric-very-long-not-permanent"])

            if reference_email is not None:
                open("adapted_reference.html", "w").write(DispatcherJobState.ignore_html_patterns(reference_email))
                assert DispatcherJobState.ignore_html_patterns(reference_email) == DispatcherJobState.ignore_html_patterns(
                    content_text_html), f"please inspect {fn} and possibly copy it to {fn.replace('to_review', 'reference')}"

            # # verify product url does not contain token
            # extracted_parsed = parse.urlparse(extracted_product_url)
            # assert 'token' not in parse_qs(extracted_parsed.query)

    url = expected_products_url.replace('PRODUCTS_URL/dispatch-data', server)

    c = requests.get(url, allow_redirects=False)

    assert c.status_code == 302
    # verify the redirect location
    redirect_url = c.headers['location']
    logger.info("redirect url: %s", redirect_url)

    dict_params_redirect_url = parse_qs(parse.urlparse(redirect_url).query)
    assert 'token' not in dict_params_redirect_url

    if not expired_token:
        dict_param.pop('token', None)
        assert all(key in dict_params_redirect_url for key in dict_param)


@pytest.mark.not_safe_parallel
@pytest.mark.test_catalog
@pytest.mark.parametrize("catalog_passage", ['file', 'params'])
def test_email_catalog(dispatcher_long_living_fixture,
                       dispatcher_local_mail_server,
                       catalog_passage
                       ):
    DispatcherJobState.remove_scratch_folders()

    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": "unige-hpc-full, general",
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    selected_catalog_dict = None
    list_file = None
    list_file_content = None
    catalog_object_dict = dict()

    # setting params
    params = {
        'query_status': "new",
        'product_type': 'dummy',
        'query_type': "Real",
        'instrument': 'empty-async',
        'token': encoded_token
    }

    if catalog_passage == 'file':
        file_path = DispatcherJobState.create_catalog_file(catalog_value=5)
        list_file = open(file_path)
        list_file_content = list_file.read()
        catalog_object_dict = BasicCatalog.from_file(file_path).get_dictionary()
    elif catalog_passage == 'params':
        catalog_object_dict = DispatcherJobState.create_catalog_object()
        params['selected_catalog'] = json.dumps(catalog_object_dict)

    jdata = ask(server,
                params,
                expected_query_status=["submitted"],
                max_time_s=150,
                method='post',
                files={"user_catalog_file": list_file_content}
                )

    if list_file is not None:
        list_file.close()
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(jdata)
    params['selected_catalog'] = json.dumps(catalog_object_dict),

    completed_dict_param = {**params,
                            'src_name': '1E 1740.7-2942',
                            'RA': 265.97845833,
                            'DEC': -29.74516667,
                            'T1': '2017-03-06T13:26:48.000',
                            'T2': '2017-03-06T15:32:27.000',
                            }

    products_url = DispatcherJobState.get_expected_products_url(completed_dict_param,
                                                                session_id=dispatcher_job_state.session_id,
                                                                job_id=dispatcher_job_state.job_id,
                                                                token=encoded_token)
    # email validation
    validate_catalog_email_content(message_record=dispatcher_local_mail_server.get_email_record(),
                                   products_url=products_url,
                                   dispatcher_live_fixture=server)


@pytest.mark.not_safe_parallel
@pytest.mark.test_email_scws_list
@pytest.mark.parametrize("use_scws_value", ['form_list', 'user_file', 'no', None, 'not_included'])
@pytest.mark.parametrize("scw_list_format", ['list', 'string', 'spaced_string'])
@pytest.mark.parametrize("call_back_action", ['done', 'failed'])
@pytest.mark.parametrize("scw_list_passage", ['file', 'params', 'both', 'not_passed'])
@pytest.mark.parametrize("scw_list_size", [1, 5, 40])
def test_email_scws_list(gunicorn_dispatcher_long_living_fixture,
                         dispatcher_local_mail_server,
                         use_scws_value,
                         scw_list_format,
                         call_back_action,
                         scw_list_passage,
                         scw_list_size
                         ):
    DispatcherJobState.remove_scratch_folders()

    server = gunicorn_dispatcher_long_living_fixture
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

    scw_list = [f"0665{i:04d}0010.001" for i in range(scw_list_size)]
    scw_list_string = ",".join(scw_list)
    scw_list_spaced_string = " ".join(scw_list)
    scw_list_file_obj = None
    ask_method = 'get' if (scw_list_passage == 'params' or
                           (scw_list_passage == 'not_passed' and use_scws_value != 'user_file')) \
        else 'post'

    if use_scws_value != 'not_included':
        params['use_scws'] = use_scws_value

    # configure the possible ways the list should be passed
    if scw_list_passage == 'file' or scw_list_passage == 'both':
        scw_list_file_path = DispatcherJobState.create_scw_list_file(list_length=scw_list_size,
                                                                     format=scw_list_format,
                                                                     scw_list=scw_list # this takes priority
                                                                     )
        scw_list_file = open(scw_list_file_path).read()
        scw_list_file_obj = {"user_scw_list_file": scw_list_file}

    if scw_list_passage == 'params' or scw_list_passage == 'both':
        if scw_list_format == 'list':
            params['scw_list'] = scw_list
        elif scw_list_format == 'string':
            params['scw_list'] = scw_list_string
        elif scw_list_format == 'spaced_string':
            params['scw_list'] = scw_list_spaced_string

    # this sets global variable
    requests.get(os.path.join(server, 'api', 'par-names'))

    def ask_here():
        return ask(server,
                   params,
                   method=ask_method,
                   max_time_s=150,
                   expected_query_status=None,
                   expected_status_code=None,
                   files=scw_list_file_obj
                   )

    logger.info("setting status to submitted")
    DataServerQuery.set_status('submitted')
    jdata = ask_here()

    logger.info("setting status to done")
    DataServerQuery.set_status('done')
    jdata_done = ask_here()

    logger.info("setting status to submitted again")
    DataServerQuery.set_status('submitted')
    
    try:
        processed_scw_list = jdata_done['products']['input_param_scw_list']['data_unit_list'][0]['meta_data']['scw_list']
    except KeyError:
        processed_scw_list = None

    error_message_scw_list_wrong_format_file = (
        'Error while setting input scw_list file : a space separated science windows list is an unsupported format, '
        'please provide it as a comme separated list')
    error_message_scw_list_wrong_format_parameter = ('a space separated science windows list is an unsupported format, '
                                                     'please provide it as a comme separated list')

    error_message_scw_list_missing_parameter = (
        'scw_list parameter was expected to be passed, but it has not been found, '
        'please check the inputs')
    error_message_scw_list_missing_file = (
        'scw_list file was expected to be passed, but it has not been found, '
        'please check the inputs')

    error_message_scw_list_found_parameter = (
        "scw_list parameter was found despite use_scws was indicating this was not provided, "
        "please check the inputs")
    error_message_scw_list_found_file = (
        'scw_list file was found despite use_scws was indicating this was not provided, '
        'please check the inputs')

    if scw_list_passage == 'not_passed' and \
            (use_scws_value == 'user_file' or use_scws_value == 'form_list'):
        error_message = error_message_scw_list_missing_file if use_scws_value == 'user_file' \
            else error_message_scw_list_missing_parameter
        assert jdata['error_message'] == error_message
        
    elif scw_list_passage == 'both' and scw_list_format != 'spaced_string':
        error_message = error_message_scw_list_found_parameter if (use_scws_value == 'user_file' or use_scws_value == 'no') \
            else error_message_scw_list_found_file
        assert jdata['error_message'] == error_message

    elif scw_list_passage == 'both' and scw_list_format == 'spaced_string':
        if use_scws_value == 'user_file' or use_scws_value == 'no':
            error_message = error_message_scw_list_found_parameter
        elif (use_scws_value == 'form_list' or use_scws_value is None or use_scws_value == 'not_included') and \
                scw_list_size == 1:
            error_message = error_message_scw_list_found_file
        else:
            error_message = error_message_scw_list_wrong_format_parameter
        assert jdata['error_message'] == error_message

    elif scw_list_passage == 'file' and use_scws_value != 'user_file' and \
            (scw_list_format != 'spaced_string' or (scw_list_format == 'spaced_string' and scw_list_size == 1)):
        error_message = error_message_scw_list_missing_parameter if use_scws_value == 'form_list' \
            else error_message_scw_list_found_file

        assert jdata['error_message'] == error_message

    elif scw_list_passage == 'file' and scw_list_format == 'spaced_string' and scw_list_size > 1:
        error_message = error_message_scw_list_missing_parameter if use_scws_value == 'form_list' \
            else error_message_scw_list_wrong_format_file
        assert jdata['error_message'] == error_message

    elif scw_list_passage == 'params' and \
            (use_scws_value == 'user_file' or use_scws_value == 'no'):
        assert jdata['error_message'] == error_message_scw_list_found_parameter

    elif scw_list_passage == 'params' and \
            scw_list_format == 'spaced_string' and scw_list_size > 1:
        assert jdata['error_message'] == error_message_scw_list_wrong_format_parameter

    else:
        if scw_list_passage == 'not_passed':
            params['use_scws'] = 'no'
        else:
            if use_scws_value is None or use_scws_value == 'user_file' or use_scws_value == 'not_included':
                params['use_scws'] = 'form_list'

            params['scw_list'] = scw_list_string
            assert 'scw_list' in jdata['products']['api_code']
            assert 'scw_list' in jdata['products']['analysis_parameters']
            assert jdata['products']['analysis_parameters']['scw_list'] == scw_list

            assert processed_scw_list == scw_list

        assert jdata['exit_status']['email_status'] == 'email sent'

        assert 'use_scws' not in jdata['products']['analysis_parameters']
        assert 'use_scws' not in jdata['products']['api_code']
        # validate email content
        dispatcher_job_state = DispatcherJobState.from_run_analysis_response(jdata)
        logger.info(f"dispatcher_job_state {dispatcher_job_state}")
        completed_dict_param = {**params,
                                'src_name': '1E 1740.7-2942',
                                'RA': 265.97845833,
                                'DEC': -29.74516667,
                                'T1': '2017-03-06T13:26:48.000',
                                'T2': '2017-03-06T15:32:27.000',
                                'T_format': 'isot'
                                }

        products_url = DispatcherJobState.get_expected_products_url(completed_dict_param,
                                                                    session_id=dispatcher_job_state.session_id,
                                                                    job_id=dispatcher_job_state.job_id,
                                                                    token=encoded_token)

        print("excpected products url:", products_url)

        # validate scw_list related content within the email
        validate_scw_list_email_content(message_record=dispatcher_local_mail_server.get_email_record(),
                                        scw_list=scw_list,
                                        request_params=params,
                                        scw_list_passage=scw_list_passage,
                                        products_url=products_url,
                                        dispatcher_live_fixture=server
                                        )

        # test also a call_back case
        dispatcher_job_state = DispatcherJobState.from_run_analysis_response(jdata)
        time_request = jdata['time_request']

        logger.info(f"running call_back")
        DataServerQuery.set_status('done')
        status = DataServerQuery.get_status()
        logger.info(f"status before call_Back is {status}")
        # this triggers email
        c = requests.get(os.path.join(server, "call_back"),
                         params=dict(
                             job_id=dispatcher_job_state.job_id,
                             session_id=dispatcher_job_state.session_id,
                             instrument_name="empty-async",
                             action=call_back_action,
                             node_id=f'node_{call_back_action}',
                             message=call_back_action,
                             token=encoded_token,
                             time_original_request=time_request
                         ))
        assert c.status_code == 200
        jdata = dispatcher_job_state.load_job_state_record(f'node_{call_back_action}', call_back_action)
        assert jdata['email_status'] == 'email sent'

        # check the email in the email folders, and that the first one was produced
        dispatcher_job_state.assert_email(state=call_back_action)

        if scw_list_passage == 'not_passed':
            params['use_scws'] = 'no'
        else:
            if use_scws_value is None or use_scws_value == 'user_file' or use_scws_value == 'not_included':
                params['use_scws'] = 'form_list'

            params['scw_list'] = scw_list_string

        # validate scw_list related content within the email
        validate_scw_list_email_content(message_record=dispatcher_local_mail_server.get_email_record(),
                                        scw_list=scw_list,
                                        request_params=params,
                                        scw_list_passage=scw_list_passage,
                                        products_url=products_url,
                                        dispatcher_live_fixture=server
                                        )


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
    DataServerQuery.set_status('submitted')
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

    params = dict(
            query_status="new",
            query_type="Real",
            instrument="empty-async",
            product_type="numerical",
            token=encoded_token,
            allow_unknown_args=True,
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
    params['use_scws'] = 'no'
    # included also default values,
    # which for the case of numerical query, is p, with a value of 10.0
    # and string_like_name

    completed_dict_param = {**params,
                            'p': 10.0,
                            'string_like_name': 'default-name',
                            'use_scws': 'no',
                            'src_name': '1E 1740.7-2942',
                            'RA': 265.97845833,
                            'DEC': -29.74516667,
                            'T1': '2017-03-06T13:26:48.000',
                            'T2': '2017-03-06T15:32:27.000',
                            'T_format': 'isot'
                            }

    products_url = DispatcherJobState.get_expected_products_url(completed_dict_param,
                                                                session_id=dispatcher_job_state.session_id,
                                                                job_id=dispatcher_job_state.job_id,
                                                                token=encoded_token)
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
        expect_api_code_attachment=unbreakable,
        variation_suffixes=["numeric-not-very-long-numerical"] if not unbreakable else [],
        require_reference_email=True
    )


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


@pytest.mark.parametrize('sb_value', [25, 25., 25.64547871216879451687311211245117852145229614585985498212321])
def test_spectral_parameter(dispatcher_live_fixture, sb_value):

    server = dispatcher_live_fixture

    dict_param = dict(
        query_status="new",
        query_type="Dummy",
        instrument="empty",
        product_type="parametrical",
        sb=sb_value
    )

    jdata = ask(server,
                dict_param,
                expected_query_status='done'
                )

    assert 'sb' in jdata['products']['analysis_parameters']
    assert float(sb_value) == jdata['products']['analysis_parameters']['sb']


@pytest.mark.parametrize('time_combinations', [[57818.560277777775, 57818.64753472222],
                                               ['2017-03-06T13:26:48.000', '2017-03-06T15:32:27.000'],
                                               ['2017-03-06T13:26:48.000', 57818.64753472222],
                                               [57818.560277777775, '2017-03-06T15:32:27.000']])
@pytest.mark.parametrize('time_format', ['isot', 'mjd'])
def test_email_t1_t2(dispatcher_long_living_fixture,
                     dispatcher_local_mail_server,
                     time_combinations,
                     time_format):
    from cdci_data_analysis.plugins.dummy_plugin.data_server_dispatcher import DataServerQuery
    DataServerQuery.set_status('submitted')

    server = dispatcher_long_living_fixture

    DispatcherJobState.remove_scratch_folders()

    token_payload = {
        **default_token_payload,
        "tem": 0
            }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    DataServerQuery.set_status('submitted')

    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        T1=time_combinations[0],
        T2=time_combinations[1],
        T_format=time_format,
        token=encoded_token
    )

    error_message = None
    if (isinstance(time_combinations[0], str) and isinstance(time_combinations[1], str) and time_format == 'isot') or \
            (isinstance(time_combinations[0], float) and isinstance(time_combinations[1], float) and time_format == 'mjd'):
        expected_query_status = 'submitted'
        expected_status_code = 200
    else:
        expected_query_status = None
        expected_status_code = 400
        if time_format == 'isot':
            par, val = ('T1', time_combinations[0]) if isinstance(time_combinations[0], float) else ('T2', time_combinations[1])
            error_message = f'Parameter {par} wrong value {val}: can\'t be parsed as Time of isot format'
        else:
            par, val = ('T1', time_combinations[0]) if isinstance(time_combinations[0], str) else ('T2', time_combinations[1])
            error_message = f'Parameter {par} wrong value {val}: can\'t be parsed as Time of mjd format'
            
    # this should return status submitted, so email sent
    jdata = ask(server,
                dict_param,
                expected_status_code=expected_status_code,
                expected_query_status=expected_query_status,
                max_time_s=150,
                )

    if expected_status_code == 200:
        logger.info("response from run_analysis: %s", json.dumps(jdata, indent=4))
        dispatcher_job_state = DispatcherJobState.from_run_analysis_response(jdata)

        session_id = jdata['session_id']
        job_id = jdata['job_monitor']['job_id']

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

        assert jdata['exit_status']['email_status'] == 'email sent'

        validate_email_content(
            dispatcher_local_mail_server.get_email_record(),
            'submitted',
            dispatcher_job_state,
            variation_suffixes=["dummy"],
            time_request_str=time_request_str,
            products_url=products_url,
            dispatcher_live_fixture=None,
        )
    else:
        assert jdata["error_message"] == error_message


@pytest.mark.parametrize("number_folders_to_delete", [1, 8])
@pytest.mark.parametrize("soft_minimum_age_days", ["not_provided", 1, 5])
@pytest.mark.parametrize("dispatcher_live_fixture", [("hard_minimum_folder_age_days", None),
                                                     ("hard_minimum_folder_age_days", 1),
                                                     ("hard_minimum_folder_age_days", 15),
                                                     ("hard_minimum_folder_age_days", 60)], indirect=True)
def test_free_up_space(dispatcher_live_fixture, number_folders_to_delete, soft_minimum_age_days):
    DispatcherJobState.remove_scratch_folders()

    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    token_payload = {
        **default_token_payload,
        "roles": ['space manager'],
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    expired_token = {
        **default_token_payload,
        "roles": ['space manager'],
        "exp": int(time.time()) - 15
    }

    params = {
        'query_status': 'new',
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token,
    }

    number_analysis_to_run = 8

    for i in range(number_analysis_to_run):
        ask(server,
            params,
            expected_query_status=["done"],
            max_time_s=150
            )

    list_scratch_dir = sorted(glob.glob("scratch_sid_*_jid_*"), key=os.path.getmtime)

    current_time = time.time()
    one_month_secs = 60 * 60 * 24 * 30
    if soft_minimum_age_days != 'not_provided':
        soft_minimum_age_days_secs = soft_minimum_age_days * 60 *60 * 24
    else:
        soft_minimum_age_days_secs = one_month_secs

    for scratch_dir in list_scratch_dir[0: number_folders_to_delete]:
        # set folders to be deleted
        os.utime(scratch_dir, (current_time, current_time - soft_minimum_age_days_secs))
        analysis_parameters_path = os.path.join(scratch_dir, 'analysis_parameters.json')
        with open(analysis_parameters_path) as analysis_parameters_file:
            dict_analysis_parameters = json.load(analysis_parameters_file)
        dict_analysis_parameters['token'] = expired_token
        with open(analysis_parameters_path, 'w') as dict_analysis_parameters_outfile:
            my_json_str = json.dumps(dict_analysis_parameters, indent=4)
            dict_analysis_parameters_outfile.write(u'%s' % my_json_str)


    params = {
        'token': encoded_token,
        'soft_minimum_age_days': soft_minimum_age_days
    }

    if soft_minimum_age_days == 'not_provided':
        params.pop('soft_minimum_age_days')

    c = requests.get(os.path.join(server, "free-up-space"), params=params)

    jdata = c.json()

    assert 'output_status' in jdata

    assert jdata['output_status'] == f"Removed {number_folders_to_delete} scratch directories"

    assert len(glob.glob("scratch_sid_*_jid_*")) == number_analysis_to_run - number_folders_to_delete

@pytest.mark.parametrize("request_cred", ['public', 'private', 'invalid_token'])
@pytest.mark.parametrize("roles", ["general, job manager", "administrator", ""])
@pytest.mark.parametrize("include_session_log", [True, False, None])
@pytest.mark.parametrize("remove_analysis_parameters_json", [True, False])
def test_inspect_status(dispatcher_live_fixture, request_cred, roles, include_session_log, remove_analysis_parameters_json):
    required_roles = ['job manager']
    DispatcherJobState.remove_scratch_folders()

    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    token_none = (request_cred == 'public')

    if token_none:
        encoded_token = None
    else:
        # let's generate a valid token
        token_payload = {
            **default_token_payload,
            "roles": roles,
        }
        encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        'query_status': 'new',
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token,
    }

    # just for having the roles in a list
    roles = roles.split(',')
    roles[:] = [r.strip() for r in roles]

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=150,
                )

    job_id = jdata['products']['job_id']
    session_id = jdata['session_id']

    scratch_dir_fn = f'scratch_sid_{session_id}_jid_{job_id}'
    if remove_analysis_parameters_json:
        os.remove(os.path.join(scratch_dir_fn, "analysis_parameters.json"))
    scratch_dir_ctime = os.stat(scratch_dir_fn).st_ctime

    assert os.path.exists(scratch_dir_fn)

    status_code = 403
    error_message = ''
    if request_cred == 'invalid_token':
        # an invalid (encoded) token, just a string
        encoded_token = 'invalid_token'
        error_message = 'The token provided is not valid.'
    elif request_cred == 'public':
        error_message = 'A token must be provided.'
    elif request_cred == 'private':
        if 'job manager' not in roles:
            lacking_roles = ", ".join(sorted(list(set(required_roles) - set(roles))))
            error_message = (
                f'Unfortunately, your privileges are not sufficient to inspect the state for a given job_id.\n'
                f'Your privilege roles include {roles}, but the following roles are missing: {lacking_roles}.'
            )

    # for the email we only use the first 8 characters
    c = requests.get(server + "/inspect-state",
                     params=dict(
                         job_id=job_id[:8],
                         token=encoded_token,
                         include_session_log=include_session_log
                     ))

    scratch_dir_mtime = os.stat(scratch_dir_fn).st_mtime

    if request_cred != 'private' or ('job manager' not in roles):
        # email not supposed to be sent for public request
        assert c.status_code == status_code
        assert c.text == error_message
    else:
        jdata= c.json()
        assert 'records' in jdata
        assert type(jdata['records']) is list
        assert len(jdata['records']) == 1

        assert jdata['records'][0]['job_id'] == job_id

        assert jdata['records'][0]['ctime'] == scratch_dir_ctime
        assert jdata['records'][0]['mtime'] == scratch_dir_mtime

        assert 'analysis_parameters' in jdata['records'][0]
        if remove_analysis_parameters_json:
            assert jdata['records'][0]['analysis_parameters'] == f"problem reading {os.path.join(scratch_dir_fn, 'analysis_parameters.json')}: FileNotFoundError(2, 'No such file or directory')"
        assert 'email_history' in jdata['records'][0]
        assert 'matrix_message_history' in jdata['records'][0]

        assert len(jdata['records'][0]['email_history']) == 0
        assert len(jdata['records'][0]['matrix_message_history']) == 0
        if include_session_log:
            assert 'session_log' in jdata['records'][0]
        else:
            assert 'session_log' not in jdata['records'][0]

        assert 'file_list' in jdata['records'][0]
        assert isinstance(jdata['records'][0]['file_list'], list)

@pytest.mark.parametrize("request_cred", ['public', 'private', 'invalid_token'])
@pytest.mark.parametrize("roles", ["general, job manager", "administrator", ""])
@pytest.mark.parametrize("pass_job_id", [True, False])
@pytest.mark.parametrize("expired_token", [True, False])
def test_inspect_jobs(dispatcher_live_fixture, request_cred, roles, pass_job_id, expired_token):
    required_roles = ['job manager']
    DispatcherJobState.remove_scratch_folders()
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)
    token_none = (request_cred == 'public')

    token_payload = {**default_token_payload}

    if token_none:
        encoded_token = None
    else:
        # let's generate a valid token
        token_payload["roles"] = roles
        if expired_token:
            token_payload["exp"] = int(time.time()) + 15
        encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        'query_status': 'new',
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token,
    }

    # just for having the roles in a list
    roles = roles.split(',')
    roles[:] = [r.strip() for r in roles]

    jdata_done = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=150,
                )

    params = {
        'query_status': 'new',
        'product_type': 'failing',
        'query_type': "Dummy",
        'instrument': 'empty',
        'token': encoded_token,
    }

    jdata_failed = ask(server,
                       params,
                       expected_query_status='failed'
                       )

    job_id_done = jdata_done['job_monitor']['job_id']
    session_id_done = jdata_done['session_id']
    job_id_failed = jdata_failed['job_monitor']['job_id']
    session_id_failed = jdata_failed['session_id']

    if expired_token:
        # let make sure the token used for the previous request expires
        time.sleep(18)

    # generate a new valid token with the same approach
    if token_none:
        encoded_token = None
    else:
        # let's generate a valid token
        token_payload["roles"] = roles
        token_payload["exp"] = int(time.time()) + 5000
        encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    status_code = 403
    error_message = ''
    if request_cred == 'invalid_token':
        # an invalid (encoded) token, just a string
        encoded_token = 'invalid_token'
        error_message = 'The token provided is not valid.'
    elif request_cred == 'public':
        error_message = 'A token must be provided.'
    elif request_cred == 'private':
        if 'job manager' not in roles:
            lacking_roles = ", ".join(sorted(list(set(required_roles) - set(roles))))
            error_message = (
                f'Unfortunately, your privileges are not sufficient to inspect the state for a given job_id.\n'
                f'Your privilege roles include {roles}, but the following roles are missing: {lacking_roles}.'
            )

    inspect_params = dict(
                         token=encoded_token
                     )
    if pass_job_id:
        inspect_params['job_id'] = job_id_done[:8]

    # for the email we only use the first 8 characters
    c = requests.get(server + "/inspect-jobs",
                     params=inspect_params)

    if request_cred != 'private' or ('job manager' not in roles):
        # email not supposed to be sent for public request
        assert c.status_code == status_code
        assert c.text == error_message
    else:
        jdata= c.json()
        assert 'jobs' in jdata
        assert type(jdata['jobs']) is list
        if not pass_job_id:
            assert len(jdata['jobs']) == 2
            assert jdata['jobs'][0]['job_id'] == job_id_done or jdata['jobs'][1]['job_id'] == job_id_done
            assert jdata['jobs'][0]['job_id'] == job_id_failed or jdata['jobs'][1]['job_id'] == job_id_failed
        else:
            assert len(jdata['jobs']) == 1
            assert jdata['jobs'][0]['job_id'] == job_id_done

        assert isinstance(jdata['jobs'][0]['job_status_data'], list)
        if not pass_job_id:
            assert isinstance(jdata['jobs'][1]['job_status_data'], list)

        assert len(jdata['jobs'][0]['job_status_data']) == 1
        if not pass_job_id:
            assert len(jdata['jobs'][1]['job_status_data']) == 1

        assert 'job_statuses' in jdata['jobs'][0]['job_status_data'][0]
        assert isinstance(jdata['jobs'][0]['job_status_data'][0]['job_statuses'], list)
        assert len(jdata['jobs'][0]['job_status_data'][0]['job_statuses']) == 1
        if not pass_job_id:
            assert 'job_statuses' in jdata['jobs'][1]['job_status_data'][0]
            assert isinstance(jdata['jobs'][1]['job_status_data'][0]['job_statuses'], list)
            assert len(jdata['jobs'][1]['job_status_data'][0]['job_statuses']) == 1

        assert 'job_statuses_fn' in jdata['jobs'][0]['job_status_data'][0]
        if not pass_job_id:
            assert 'job_statuses_fn' in jdata['jobs'][1]['job_status_data'][0]
            assert (jdata['jobs'][0]['job_status_data'][0]['job_statuses_fn'] ==
                    f'scratch_sid_{session_id_done}_jid_{job_id_done}' or
                    jdata['jobs'][0]['job_status_data'][0]['job_statuses_fn'] ==
                    f'scratch_sid_{session_id_failed}_jid_{job_id_failed}')
            assert (jdata['jobs'][1]['job_status_data'][0]['job_statuses_fn'] ==
                    f'scratch_sid_{session_id_done}_jid_{job_id_done}' or
                   jdata['jobs'][1]['job_status_data'][0]['job_statuses_fn'] ==
                    f'scratch_sid_{session_id_failed}_jid_{job_id_failed}')
        else:
            assert jdata['jobs'][0]['job_status_data'][0]['job_statuses_fn'] == f'scratch_sid_{session_id_done}_jid_{job_id_done}'

        assert jdata['jobs'][0]['job_status_data'][0]['job_statuses'][0]['job_status_file'] == 'job_monitor.json'
        if not pass_job_id:
            assert jdata['jobs'][1]['job_status_data'][0]['job_statuses'][0]['job_status_file'] == 'job_monitor.json'
        if not pass_job_id:
            assert jdata['jobs'][0]['job_status_data'][0]['job_statuses'][0]['status'] == 'done' or \
                   jdata['jobs'][1]['job_status_data'][0]['job_statuses'][0]['status'] == 'done'

            assert jdata['jobs'][0]['job_status_data'][0]['job_statuses'][0]['status'] == 'failed' or \
                   jdata['jobs'][1]['job_status_data'][0]['job_statuses'][0]['status'] == 'failed'
        else:
            assert jdata['jobs'][0]['job_status_data'][0]['job_statuses'][0]['status'] == 'done'

        assert 'token_expired' in jdata['jobs'][0]['job_status_data'][0]
        assert jdata['jobs'][0]['job_status_data'][0]['token_expired'] == expired_token
        if not pass_job_id:
            assert 'token_expired' in jdata['jobs'][1]['job_status_data'][0]
            assert jdata['jobs'][1]['job_status_data'][0]['token_expired'] == expired_token


def test_inspect_jobs_with_callbacks(gunicorn_dispatcher_long_living_fixture):
    server = gunicorn_dispatcher_long_living_fixture
    token_payload = {**default_token_payload, "roles": 'job manager'}
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    DataServerQuery.set_status('submitted')
    DispatcherJobState.remove_scratch_folders()
    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        token=encoded_token
    )

    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )
    jdata = c.json()
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(jdata)
    time_request = jdata['time_request']
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

    c = requests.get(os.path.join(server, "run_analysis"),
                     {**dict_param,
                      "query_status": "submitted",
                      "job_id": dispatcher_job_state.job_id,
                      "session_id": dispatcher_job_state.session_id,
                      }
                     )

    c = requests.get(os.path.join(server, "call_back"),
                    params=dict(
                        job_id=dispatcher_job_state.job_id,
                        session_id=dispatcher_job_state.session_id,
                        instrument_name="empty-async",
                        action='main_incorrect_status',
                        node_id=f'node_{i+1}',
                        message='progressing',
                        token=encoded_token,
                        time_original_request=time_request
                    ))
    DataServerQuery.set_status('done')

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

    inspect_params = dict(
        token=encoded_token
    )
    c = requests.get(server + "/inspect-jobs",
                     params=inspect_params)

    jdata_inspection = c.json()
    print(json.dumps(jdata_inspection, indent=4, sort_keys=True))


@pytest.mark.parametrize("request_cred", ['public', 'valid_token', 'invalid_token'])
def test_incident_report(dispatcher_live_fixture, dispatcher_local_mail_server, dispatcher_test_conf, request_cred):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    params = {
        'query_status': 'new',
        'product_type': 'dummy',
        'query_type': "Dummy",
        'instrument': 'empty',
    }
    encoded_token = None
    decoded_token = None
    error_message = None

    if request_cred == 'invalid_token':
        # an invalid (encoded) token, just a string
        encoded_token = 'invalid_token'
        error_message = 'The token provided is not valid.'
    elif request_cred == 'public':
        error_message = 'A token must be provided.'
    elif request_cred == 'valid_token':
        decoded_token = default_token_payload
        encoded_token = jwt.encode(decoded_token, secret_key, algorithm='HS256')
        params['token'] = encoded_token

    jdata = ask(server,
                params,
                expected_query_status=["done"],
                max_time_s=150,
                )

    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(jdata)
    time_request = jdata['time_request']
    time_request_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(time_request)))

    scratch_dir_fn_list = glob.glob(f'scratch_sid_{dispatcher_job_state.session_id}_jid_{dispatcher_job_state.job_id}*')
    scratch_dir_fn = max(scratch_dir_fn_list, key=os.path.getctime)

    incident_content = 'test incident'

    # for the email we only use the first 8 characters
    c = requests.post(os.path.join(server, "report_incident"),
                      params=dict(
                          job_id=dispatcher_job_state.job_id,
                          session_id=dispatcher_job_state.session_id,
                          token=encoded_token,
                          incident_content=incident_content,
                          incident_time=time_request,
                          scratch_dir=scratch_dir_fn
                      ))

    if request_cred != 'valid_token':
        # email not supposed to be sent for public request
        assert c.status_code == 403
        assert c.text == error_message
    else:
        jdata_incident_report = c.json()

        assert 'email_report_status' in jdata_incident_report

        validate_incident_email_content(
            dispatcher_local_mail_server.get_email_record(),
            dispatcher_test_conf,
            dispatcher_job_state,
            incident_time_str=time_request_str,
            incident_report_str=incident_content,
            decoded_token=decoded_token
        )


@pytest.mark.not_safe_parallel
def test_session_log(dispatcher_live_fixture):
    server = dispatcher_live_fixture

    DispatcherJobState.remove_scratch_folders()

    token_payload = {**default_token_payload
                     }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        p=15,
        token=encoded_token
    )

    # this should return status submitted, so matrix message sent
    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )
    assert c.status_code == 200
    jdata = c.json()

    session_id = jdata['session_id']
    job_id = jdata['job_monitor']['job_id']
    scratch_dir_fn = f'scratch_sid_{session_id}_jid_{job_id}'
    session_log_fn = os.path.join(scratch_dir_fn, 'session.log')
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(jdata)

    assert os.path.exists(session_log_fn)

    with open(session_log_fn) as session_log_fn_f:
        session_log_content = session_log_fn_f.read()

    assert '==============================> run query <==============================' in session_log_content
    assert "'p': '15'," in session_log_content

    time_request = jdata['time_request']

    requests.get(os.path.join(server, "call_back"),
                 params=dict(
                     job_id=dispatcher_job_state.job_id,
                     session_id=dispatcher_job_state.session_id,
                     instrument_name="empty-async",
                     action='progress',
                     node_id='node_0',
                     message='progressing',
                     token=encoded_token,
                     time_original_request=time_request
                 ))

    with open(session_log_fn) as session_log_fn_f:
        session_log_content = session_log_fn_f.read()

    assert '.run_call_back with args ' in session_log_content
    assert "'p': '15'," in session_log_content

    # second run_analysis within the same running session, but resulting a different scratch_dir and therefore session_log
    dict_param = dict(
        query_status="new",
        query_type="Real",
        instrument="empty-async",
        product_type="dummy",
        p=35,
        token=encoded_token
    )

    c = requests.get(os.path.join(server, "run_analysis"),
                     dict_param
                     )
    assert c.status_code == 200
    jdata = c.json()

    session_id = jdata['session_id']
    job_id = jdata['job_monitor']['job_id']
    scratch_dir_fn = f'scratch_sid_{session_id}_jid_{job_id}'
    session_log_fn = os.path.join(scratch_dir_fn, 'session.log')

    assert os.path.exists(session_log_fn)

    with open(session_log_fn) as session_log_fn_f:
        session_log_content = session_log_fn_f.read()

    assert '==============================> run query <==============================' in session_log_content
    assert "'p': '35'," in session_log_content
    assert "'p': '15'," not in session_log_content
