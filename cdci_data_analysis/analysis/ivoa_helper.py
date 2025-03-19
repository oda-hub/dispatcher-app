import os.path

import numpy as np
import psycopg2.extensions
from queryparser.adql import ADQLQueryTranslator
from queryparser.exceptions import QuerySyntaxError

from psycopg2 import connect, DatabaseError

from ..app_logging import app_logging
from ..analysis.exceptions import RequestNotUnderstood

from astropy.io.votable.tree import VOTableFile, Resource, Field, Table

logger = app_logging.getLogger('ivoa_helper')


def map_psql_type_to_vo_datatype(type_code):
    type_db = psycopg2.extensions.string_types[type_code]
    if type_db.name == 'LONGINTEGER':
        return 'int'
    elif type_db.name == 'STRING':
        return 'char'
    elif type_db.name == 'FLOAT':
        return 'double'
    elif type_db.name == 'BOOLEAN':
        return 'boolean'
    return 'char'


def map_psql_null_to_vo_default_value(datatype):
    if datatype == 'char':
        return ""
    elif datatype in 'int':
        return -1
    elif datatype in 'double':
        return np.nan


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


def run_metadata_query(query, **kwargs):
    parsed_query_obj = parse_adql_query(query)

    logger.info('Performing metadata query on the product_gallery')
    vo_psql_pg_host = kwargs.get('vo_psql_pg_host', None)
    vo_psql_pg_port = kwargs.get('vo_psql_pg_port', None)
    vo_psql_pg_user = kwargs.get('vo_psql_pg_user', None)
    vo_psql_pg_password = kwargs.get('vo_psql_pg_password', None)
    vo_psql_pg_db = kwargs.get('vo_psql_pg_db', None)
    result_query = run_metadata_query_from_product_gallery(parsed_query_obj,
                                                           vo_psql_pg_host=vo_psql_pg_host,
                                                           vo_psql_pg_port=vo_psql_pg_port,
                                                           vo_psql_pg_user=vo_psql_pg_user,
                                                           vo_psql_pg_password=vo_psql_pg_password,
                                                           vo_psql_pg_db=vo_psql_pg_db)
    return result_query

def run_adql_query(query, **kwargs):
    parsed_query_obj = parse_adql_query(query)

    logger.info('Performing query on the product_gallery')
    vo_psql_pg_host = kwargs.get('vo_psql_pg_host', None)
    vo_psql_pg_port = kwargs.get('vo_psql_pg_port', None)
    vo_psql_pg_user = kwargs.get('vo_psql_pg_user', None)
    vo_psql_pg_password = kwargs.get('vo_psql_pg_password', None)
    vo_psql_pg_db = kwargs.get('vo_psql_pg_db', None)
    product_gallery_url = kwargs.get('product_gallery_url', None)
    result_query = run_query_from_product_gallery(parsed_query_obj,
                                                       vo_psql_pg_host=vo_psql_pg_host,
                                                       vo_psql_pg_port=vo_psql_pg_port,
                                                       vo_psql_pg_user=vo_psql_pg_user,
                                                       vo_psql_pg_password=vo_psql_pg_password,
                                                       vo_psql_pg_db=vo_psql_pg_db,
                                                       product_gallery_url=product_gallery_url)
    return result_query


def run_query_from_product_gallery(parsed_query_obj,
                                   vo_psql_pg_host,
                                   vo_psql_pg_port,
                                   vo_psql_pg_user,
                                   vo_psql_pg_password,
                                   vo_psql_pg_db,
                                   product_gallery_url=None
                                   ):
    connection = None

    # Create a new VOTable file with one resource and one table
    votable = VOTableFile()
    resource = Resource()
    votable.resources.append(resource)
    table = Table(votable)
    resource.tables.append(table)

    try:
        with connect(
            host=vo_psql_pg_host,
            port=vo_psql_pg_port,
            database=vo_psql_pg_db,
            user=vo_psql_pg_user,
            password=vo_psql_pg_password
        ) as connection:
            db_query = parsed_query_obj.get('psql_query')
            with connection.cursor() as cursor:
                cursor.execute(db_query)
                data = cursor.fetchall()
                for r_index, row in enumerate(data):
                    table_row = list(row)
                    table_entry = [""] * len(table_row)
                    for v_index, value in enumerate(table_row):
                        # Get the column description and its corresponding datatype and default value in case of null in the DB
                        # then create the field in the VOTable obj
                        description = cursor.description[v_index]
                        datatype = map_psql_type_to_vo_datatype(description.type_code)
                        default_no_value = map_psql_null_to_vo_default_value(datatype)
                        if r_index == 0:
                            f = Field(votable, ID=description.name, name=description.name, datatype=datatype, arraysize="*")
                            f.values.null = default_no_value
                            table.fields.append(f)
                        if value is None:
                            table_entry[v_index] = default_no_value
                        else:
                            table_entry[v_index] = value

                        if product_gallery_url is not None:
                            if description.name in {'file_uri', 'file_name', 'image_name', 'image_uri'} and value is not None and isinstance(value, str):
                                value_list = [v.strip() for v in value.split(',')]
                                if description.name == 'file_uri' or description.name == 'image_uri':
                                    for file_id, file_name in enumerate(value_list):
                                        value_list[file_id] = os.path.join(product_gallery_url, 'sites/default/files/', file_name.strip())
                                table_entry[v_index] = ",".join(value_list)
                            if description.name in {'path', 'path_alias'} and value is not None and isinstance(value, str):
                                if value.startswith('/'):
                                    value = value[1:]
                                table_entry[v_index] = os.path.join(product_gallery_url, value)
                    if r_index == 0:
                        table.create_arrays(len(data))
                    table.array[r_index] = tuple(table_entry)

    except (Exception, DatabaseError) as e:
        logger.error(f"Error when querying to the Postgresql server: {str(e)}")
        raise e

    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            logger.info('Database connection closed')

    votable.to_xml('output.xml')
    with open('output.xml', 'r') as f:
        votable_xml_output = f.read()

    return votable_xml_output

def run_metadata_query_from_product_gallery(parsed_query_obj,
                                            vo_psql_pg_host,
                                            vo_psql_pg_port,
                                            vo_psql_pg_user,
                                            vo_psql_pg_password,
                                            vo_psql_pg_db,
                                            ):
    table_list = []

    try:
        with connect(
            host=vo_psql_pg_host,
            port=vo_psql_pg_port,
            database=vo_psql_pg_db,
            user=vo_psql_pg_user,
            password=vo_psql_pg_password
        ) as connection:
            db_query = parsed_query_obj.get('psql_query')
            with connection.cursor() as cursor:
                cursor.execute(db_query)
                data = cursor.fetchall()


    except (Exception, DatabaseError) as e:
        logger.error(f"Error when querying to the Postgresql server: {str(e)}")
        raise e

    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            logger.info('Database connection closed')
