import os.path
from audioop import error

from queryparser.adql import ADQLQueryTranslator
from queryparser.exceptions import QuerySyntaxError

from psycopg2 import connect, DatabaseError

from ..flask_app.sentry import sentry
from ..app_logging import app_logging
from ..analysis.exceptions import RequestNotUnderstood

logger = app_logging.getLogger('ivoa_helper')


def parse_adql_query(query):
    try:
        adt = ADQLQueryTranslator(query)

        output_obj = dict(
            mysql_query=None,
            psql_query=adt.to_postgresql()
        )

    except QuerySyntaxError as qe:
        logger.error(f'Error while parsing the ADQL query: {str(qe)}')
        raise RequestNotUnderstood(f"Error while parsing the ADQL query: {str(qe)}")
    return output_obj


def run_ivoa_query(query, **kwargs):
    parsed_query_obj = parse_adql_query(query)

    # TODO use a specific dedicated table and schema to refer to the product_gallery DB ?
    # tables = parsed_query_obj.get('tables', [])
    # if len(tables) == 1 and tables[0] == 'product_gallery':
    logger.info('Performing query on the product_gallery')
    vo_psql_pg_host = kwargs.get('vo_psql_pg_host', None)
    vo_psql_pg_user = kwargs.get('vo_psql_pg_user', None)
    vo_psql_pg_password = kwargs.get('vo_psql_pg_password', None)
    vo_psql_pg_db = kwargs.get('vo_psql_pg_db', None)
    product_gallery_url = kwargs.get('product_gallery_url', None)
    result_list = run_ivoa_query_from_product_gallery(parsed_query_obj,
                                                      vo_psql_pg_host=vo_psql_pg_host,
                                                      vo_psql_pg_user=vo_psql_pg_user,
                                                      vo_psql_pg_password=vo_psql_pg_password,
                                                      vo_psql_pg_db=vo_psql_pg_db,
                                                      product_gallery_url=product_gallery_url)
    return result_list


def run_ivoa_query_from_product_gallery(parsed_query_obj,
                                        vo_psql_pg_host,
                                        vo_psql_pg_user,
                                        vo_psql_pg_password,
                                        vo_psql_pg_db,
                                        product_gallery_url=None
                                        ):
    result_list = []
    connection = None
    try:
        with connect(
            host=vo_psql_pg_host,
            database=vo_psql_pg_db,
            user=vo_psql_pg_user,
            password=vo_psql_pg_password
        ) as connection:
            db_query = parsed_query_obj.get('psql_query')
            with connection.cursor() as cursor:
                cursor.execute(db_query)
                for row in cursor:
                    list_row = list(row)
                    if product_gallery_url is not None:
                        for index, value in enumerate(list_row):
                            description = cursor.description[index]
                            if description.name in {'path', 'path_alias'} and value is not None and isinstance(value, str):
                                if value.startswith('/'):
                                    value = value[1:]
                                list_row[index] = os.path.join(product_gallery_url, value)
                    result_list.append(list_row)

    except (Exception, DatabaseError) as e:
        logger.error(f"Error when querying to the Postgresql server: {str(e)}")
        raise e

    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            logger.info('Database connection closed')

    return result_list
