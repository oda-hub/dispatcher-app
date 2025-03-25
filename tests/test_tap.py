import os.path
import logging
import pytest

from astroquery.utils.tap.core import TapPlus

logger = logging.getLogger(__name__)

from pytest_postgresql import factories
from pathlib import Path

postgresql_fixture = factories.postgresql_proc(
    host="localhost",
    port=5435,
    user="postgres",
    dbname="gallery_dev_prod",
    password="postgres",
    load=[Path(os.path.join(os.path.dirname(__file__), 'gallery_pg_db_data/pg_gallery_db_init_dump.sql'))],
)

postgresql = factories.postgresql(
    "postgresql_fixture",
)

@pytest.fixture
def alter_db_search_path(dispatcher_test_conf_with_vo_options, postgresql):
    with postgresql.cursor() as cur:
        cur.execute(f"ALTER DATABASE {dispatcher_test_conf_with_vo_options['vo_options']['vo_psql_pg_db']} SET search_path TO mmoda_pg_dev, public;")
        postgresql.commit()


@pytest.fixture
def fill_up_db(dispatcher_test_conf_with_vo_options, postgresql, alter_db_search_path):
    with postgresql.cursor() as cur:
        with open(os.path.join(os.path.dirname(__file__), 'gallery_pg_db_data/pg_gallery_db_init_data_products.sql')) as f:
            cur.execute(f.read())
        postgresql.commit()


@pytest.mark.test_tap
def test_local_tap_sync_job_empty_db(dispatcher_live_fixture_with_tap, alter_db_search_path):
    server = dispatcher_live_fixture_with_tap
    tap_query = f"SELECT * FROM data_product_table_view_v"

    oda_tap = TapPlus(url=os.path.join(server, "tap"))

    ts = oda_tap.launch_job(tap_query)

    print(ts)

    r = ts.get_results()

    assert len(r) == 0


@pytest.mark.test_tap
def test_local_tap_sync_job(dispatcher_live_fixture_with_tap, fill_up_db):
    server = dispatcher_live_fixture_with_tap
    number_results = 5
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