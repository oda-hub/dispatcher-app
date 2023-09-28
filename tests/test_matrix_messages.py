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
from urllib.parse import parse_qs, urlencode, urlparse
import glob

from collections import OrderedDict

from cdci_data_analysis.analysis.catalog import BasicCatalog
from cdci_data_analysis.pytest_fixtures import DispatcherJobState, make_hash, ask
from cdci_data_analysis.analysis.email_helper import textify_email
from cdci_data_analysis.plugins.dummy_plugin.data_server_dispatcher import DataServerQuery

from oda_api.api import RemoteException
from datetime import datetime

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


@pytest.mark.parametrize("default_values", [True, False])
@pytest.mark.parametrize("time_original_request_none", [False])
@pytest.mark.parametrize("request_cred", ['public', 'private', 'private-no-matrix-message'])
def test_matrix_message_run_analysis_callback(dispatcher_live_fixture,
                                              default_values, request_cred, time_original_request_none):
    from cdci_data_analysis.plugins.dummy_plugin.data_server_dispatcher import DataServerQuery
    DataServerQuery.set_status('submitted')

    server = dispatcher_live_fixture

    DispatcherJobState.remove_scratch_folders()

    token_none = (request_cred == 'public')

    expect_matrix_message = True
    token_payload = {
        **default_token_payload,
        "tem": 0
    }

    if token_none:
        encoded_token = None
    else:
        # let's generate a valid token with high threshold

        if default_values:
            token_payload.pop('mxsub')
            token_payload.pop('mxintsub')

        if request_cred == 'private-no-matrix-message':
            token_payload['mxsub'] = False
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

    logger.info("response from run_analysis: %s", json.dumps(jdata, indent=4))
    dispatcher_job_state = DispatcherJobState.from_run_analysis_response(c.json())

    assert jdata['query_status'] == "submitted"

    # session_id = jdata['session_id']
    # job_id = jdata['job_monitor']['job_id']
    #
    # completed_dict_param = {**dict_param,
    #                         'use_scws': 'no',
    #                         'src_name': '1E 1740.7-2942',
    #                         'RA': 265.97845833,
    #                         'DEC': -29.74516667,
    #                         'T1': '2017-03-06T13:26:48.000',
    #                         'T2': '2017-03-06T15:32:27.000',
    #                         'T_format': 'isot'
    #                         }

    # products_url = get_expected_products_url(completed_dict_param,
    #                                          token=encoded_token,
    #                                          session_id=session_id,
    #                                          job_id=job_id)
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