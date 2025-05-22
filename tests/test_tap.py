import os.path
import logging
from symbol import return_stmt

import pytest
import pyvo

from pytest_postgresql import factories
from psycopg2 import connect, DatabaseError
from pathlib import Path

logger = logging.getLogger(__name__)

test_psql_with_image = os.getenv("TEST_PSQL_WITH_IMAGE", "no") == "yes"
test_psql_host = os.getenv("TEST_PSQL_HOST", "localhost")
test_psql_port = os.getenv("TEST_PSQL_PORT", "5435")
test_psql_user = os.getenv("TEST_PSQL_USER", "postgres")
test_psql_pass = os.getenv("TEST_PSQL_PASS", "postgres")
test_psql_dbname = os.getenv("TEST_PSQL_DBNAME", "mmoda_pg_db")

postgresql_fixture = factories.postgresql_proc(
    host=test_psql_host,
    port=test_psql_port,
    user=test_psql_user,
    dbname=test_psql_dbname,
    password=test_psql_pass,
    load=[Path(os.path.join(os.path.dirname(__file__), 'gallery_pg_db_data/pg_gallery_db_init_dump.sql'))],
)

def postgresql_fixture_factory(test_psql_host_from_env,
                               test_psql_port_from_env,
                               test_psql_user_from_env,
                               test_psql_pass_from_env,
                               test_psql_dbname_from_env):
    @pytest.fixture
    def _container_postgresql_fixture():
        try:
            with connect(
                    host=test_psql_host_from_env,
                    port=test_psql_port_from_env,
                    user=test_psql_user_from_env,
                    password=test_psql_pass_from_env
            ) as connection:
                with connection.cursor() as cursor:
                    try:
                        connection.autocommit = True
                        cursor.execute(f"CREATE DATABASE {test_psql_dbname}")
                        connection.commit()
                    except DatabaseError as e:
                        logger.error(f"Error during the database creation: {e}")
                        raise
                connection.close()
        except (Exception, DatabaseError) as e:
            logger.error(f"Error when querying to the Postgresql server: {str(e)}")
            raise e

        finally:
            if connection is not None:
                cursor.close()
                connection.close()
                logger.info('Database connection closed')

        try:
            with connect(
                    host=test_psql_host_from_env,
                    port=test_psql_port_from_env,
                    database=test_psql_dbname_from_env,
                    user=test_psql_user_from_env,
                    password=test_psql_pass_from_env
            ) as connection:
                with connection.cursor() as cursor:
                    try:
                        cursor.execute(open(os.path.join(os.path.dirname(__file__), 'gallery_pg_db_data/pg_gallery_db_init_dump.sql')))
                        connection.commit()
                    except DatabaseError as e:
                        logger.error(f"Error initializing the database: {e}")
                        raise
                yield connection
        except (Exception, DatabaseError) as e:
            logger.error(f"Error when querying to the Postgresql server: {str(e)}")
            raise e

        finally:
            if connection is not None:
                cursor.close()
                connection.close()
                logger.info('Database connection closed')
    return _container_postgresql_fixture


if test_psql_with_image:
    postgresql = postgresql_fixture_factory(test_psql_host,
                                            test_psql_port,
                                            test_psql_user,
                                            test_psql_pass,
                                            test_psql_dbname)
else:
    postgresql = factories.postgresql("postgresql_fixture")

# @pytest.fixture
# def postgresql_fixture_altered_db_search_path(dispatcher_test_conf_with_vo_options, postgresql_fixture):
#     with postgresql.cursor() as cur:
#         cur.execute(f"ALTER DATABASE {dispatcher_test_conf_with_vo_options['vo_options']['vo_psql_pg_db']} SET search_path TO ivoa, public;")
#         postgresql.commit()


@pytest.fixture
def fill_up_db(dispatcher_test_conf_with_vo_options, postgresql):
    with postgresql.cursor() as cur:
        with open(os.path.join(os.path.dirname(__file__), 'gallery_pg_db_data/pg_gallery_db_init_data_products.sql')) as f:
            cur.execute(f.read())
        postgresql.commit()


@pytest.mark.test_tap
def test_local_tap_sync_job_empty_db(dispatcher_live_fixture_with_tap, postgresql):
    server = dispatcher_live_fixture_with_tap
    tap_query = "SELECT * FROM ivoa.obscore"

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
def test_local_tap_load_tables(dispatcher_live_fixture_with_tap, postgresql):
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

