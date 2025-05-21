import os.path
import logging
import pytest
import pyvo

from pytest_postgresql import factories
from pathlib import Path


logger = logging.getLogger(__name__)

# TODO find a way to parametrize this call
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
def postgresql_fixture_altered_db_search_path(dispatcher_test_conf_with_vo_options, postgresql):
    with postgresql.cursor() as cur:
        cur.execute(f"ALTER DATABASE {dispatcher_test_conf_with_vo_options['vo_options']['vo_psql_pg_db']} SET search_path TO mmoda_pg_dev, public;")
        postgresql.commit()


@pytest.fixture
def fill_up_db(dispatcher_test_conf_with_vo_options, postgresql, postgresql_fixture_altered_db_search_path):
    with postgresql.cursor() as cur:
        with open(os.path.join(os.path.dirname(__file__), 'gallery_pg_db_data/pg_gallery_db_init_data_products.sql')) as f:
            cur.execute(f.read())
        postgresql.commit()


@pytest.mark.test_tap
def test_local_tap_sync_job_empty_db(dispatcher_live_fixture_with_tap, postgresql_fixture_altered_db_search_path):
    server = dispatcher_live_fixture_with_tap
    tap_query = f"SELECT * FROM ivoa.obscore"

    oda_tap = pyvo.dal.TAPService(os.path.join(server, "tap"))

    result = oda_tap.search(tap_query)

    print(result)

    assert len(result) == 0


@pytest.mark.test_tap
def test_local_tap_sync_job(dispatcher_live_fixture_with_tap, fill_up_db):
    server = dispatcher_live_fixture_with_tap
    number_results = 7
    tap_query = f"SELECT TOP {number_results} * FROM ivoa.obscore"

    oda_tap = pyvo.dal.TAPService(os.path.join(server, "tap"))

    result = oda_tap.search(tap_query)

    print(result)

    assert len(result) == number_results


@pytest.mark.test_tap
def test_local_tap_load_tables(dispatcher_live_fixture_with_tap, postgresql_fixture_altered_db_search_path):
    server = dispatcher_live_fixture_with_tap
    number_results = 1
    column_names = ['obs_title', 'product_path', 'em_min', 'em_max', 'time_bin', 'instrument_name', 'target_name', 'target_id', 's_ra', 's_dec', 'dataproduct_type', 't_min', 't_max', 'proposal_id', 'target_name', 'access_url', 'image_uri']

    oda_tap = pyvo.dal.TAPService(os.path.join(server, "tap"))

    tables = oda_tap.tables

    print(tables)

    assert len(tables) == number_results

    tab_names = [tab_name for tab_name in tables.keys()]

    assert tab_names[0] == 'obscore'

    table_obj = list(tables.items())
    assert table_obj[0][1].description == 'This is the table of the data_products of the gallery'
    for column in table_obj[0][1].columns:
        assert column.name in column_names
        assert column.description is not None and column.description == f"{column.name} of the data product"

