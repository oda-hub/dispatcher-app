import os.path

from queryparser.adql import ADQLQueryTranslator
from queryparser.mysql import MySQLQueryProcessor
from queryparser.exceptions import QuerySyntaxError

from mysql.connector import connect, Error

from ..flask_app.sentry import sentry
from ..app_logging import app_logging

logger = app_logging.getLogger('ivoa_helper')


def parse_adql_query(query):
    try:
        # queryparser
        adt = ADQLQueryTranslator(query)
        qp = MySQLQueryProcessor()
        qp.set_query(adt.to_mysql())
        qp.process_query()

        output_obj = dict(
            columns=qp.display_columns,
            tables=qp.tables,
            rest=qp,
            mysql_query=qp.query
        )

    except QuerySyntaxError as qe:
        logger.error(f'Error parsing ADQL query: {qe}')
        output_obj = dict(
            tables=None,
            columns=None,
            rest=None,
            mysql_query=None
        )
    return output_obj


def run_ivoa_query(query, **kwargs):
    parsed_query_obj = parse_adql_query(query)

    # TODO use a specific dedicated table and schema to refer to the product_gallery DB ?
    # tables = parsed_query_obj.get('tables', [])
    # if len(tables) == 1 and tables[0] == 'product_gallery':
    logger.info('Performing query on the product_gallery')
    vo_mysql_pg_host = kwargs.get('vo_mysql_pg_host', None)
    vo_mysql_pg_user = kwargs.get('vo_mysql_pg_user', None)
    vo_mysql_pg_password = kwargs.get('vo_mysql_pg_password', None)
    vo_mysql_pg_db = kwargs.get('vo_mysql_pg_db', None)
    product_gallery_url = kwargs.get('product_gallery_url', None)
    result_list = run_ivoa_query_from_product_gallery(parsed_query_obj,
                                                      vo_mysql_pg_host=vo_mysql_pg_host,
                                                      vo_mysql_pg_user=vo_mysql_pg_user,
                                                      vo_mysql_pg_password=vo_mysql_pg_password,
                                                      vo_mysql_pg_db=vo_mysql_pg_db,
                                                      product_gallery_url=product_gallery_url)
    return result_list


def run_ivoa_query_from_product_gallery(parsed_query_obj,
                                        vo_mysql_pg_host,
                                        vo_mysql_pg_user,
                                        vo_mysql_pg_password,
                                        vo_mysql_pg_db,
                                        product_gallery_url=None
                                        ):
    result_list = []

    try:
        with connect(
                host=vo_mysql_pg_host,
                user=vo_mysql_pg_user,
                password=vo_mysql_pg_password,
                database=vo_mysql_pg_db
        ) as connection:
            db_query = parsed_query_obj.get('mysql_query')
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(db_query)
                for row in cursor:
                    if product_gallery_url is not None:
                        path = row.get('path', None)
                        if path is not None:
                            if path.startswith('/'):
                                path = path[1:]
                            row['path'] = os.path.join(product_gallery_url, path)
                        path_alias = row.get('path_alias', None)
                        if path_alias is not None:
                            if path_alias.startswith('/'):
                                path_alias = path_alias[1:]
                            row['path_alias'] = os.path.join(product_gallery_url, path_alias)
                    result_list.append(row)

    except Error as e:
        sentry.capture_message(f"Error when connecting to MySQL: {str(e)}")
        logger.error(f"Error when connecting to MySQL: {str(e)}")

    except Exception as e:
        sentry.capture_message(f"Error when performing the mysql query to the product_gallery DB: {str(e)}")
        logger.error(f"Error when performing the mysql query to the product_gallery DB: {str(e)}")

    finally:
        if connection is not None and connection.is_connected():
            connection.close()
            logger.info('MySQL connection closed')

    return result_list
