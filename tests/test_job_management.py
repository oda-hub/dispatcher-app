import requests

import pytest

def test_construct_for_callback(app):
    from cdci_data_analysis.flask_app.dispatcher_query import InstrumentQueryBackEnd

    query = InstrumentQueryBackEnd(
              app, 
              instrument_name='mock', 
              data_server_call_back=True)

    assert query

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
                        instrument="empty",
                        product_type="dummy",                        
                        async_dispatcher=False,
                    ))

    print("response from run_analysis:", c.text)    

    assert c.status_code == 200        

    c = requests.get(server + "/call_back",
                   params={
                       'job_id': 'test-job-id',
                       'instrument_name': 'test-instrument_name',
                   },
                )

    print(c.text)    

    assert c.status_code == 200
