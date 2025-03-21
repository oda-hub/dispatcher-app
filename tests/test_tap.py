import os.path
import time
import logging
import pytest

from astroquery.utils.tap.core import TapPlus

# logger
logger = logging.getLogger(__name__)
# symmetric shared secret for the decoding of the token
secret_key = 'secretkey_test'

"""
this will reproduce the entire flow of frontend-dispatcher, apart from receiving callback
"""

default_params = dict(
                    query_status="new",
                    query_type="Real",
                    instrument="isgri",
                    product_type="isgri_image",
                    osa_version="OSA10.2",
                    E1_keV=20.,
                    E2_keV=40.,
                    T1="2008-01-01T11:11:11.000",
                    T2="2009-01-01T11:11:11.000",
                    T_format='isot',
                    max_pointings=2,
                    RA=83,
                    DEC=22,
                    radius=6,
                    async_dispatcher=False
                 )

specific_args = ['osa_version', 'E1_keV', 'E2_keV', 'max_pointings', 'radius']
def remove_args_from_dic(arg_dic, remove_keys):
    for key in remove_keys:
        arg_dic.pop(key, None)

default_exp_time = int(time.time()) + 5000
default_token_payload = dict(
    sub="mtm@mtmco.net",
    name="mmeharga",
    roles="general",
    exp=default_exp_time,
    tem=0,
)

@pytest.mark.test_tap
def test_local_tap_sync_job(dispatcher_live_fixture_with_tap):
    server = dispatcher_live_fixture_with_tap
    number_results = 15
    tap_query = f"SELECT TOP {number_results} * FROM data_product_table_view_v WHERE DISTANCE(POINT(308.107, 40.9577), POINT(ra, dec)) < 107474700"

    oda_tap = TapPlus(url=os.path.join(server, "tap"))

    ts = oda_tap.launch_job(tap_query)

    print(ts)

    r = ts.get_results()

    assert len(r) == number_results


@pytest.mark.test_tap
def test_local_tap_load_tables(dispatcher_live_fixture_with_tap):
    server = dispatcher_live_fixture_with_tap

    oda_tap = TapPlus(url=os.path.join(server, "tap"))

    tables = oda_tap.load_tables()

    print(tables)