import os.path

import numpy as np
import psycopg2.extensions
from queryparser.adql import ADQLQueryTranslator
from queryparser.exceptions import QuerySyntaxError

from psycopg2 import connect, DatabaseError

from ..app_logging import app_logging
from ..analysis.exceptions import RequestNotUnderstood

from astropy.io.votable.tree import VOTableFile, Resource, Field, Table, Values

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

    logger.info('Performing query on the product_gallery')
    vo_psql_pg_host = kwargs.get('vo_psql_pg_host', None)
    vo_psql_pg_port = kwargs.get('vo_psql_pg_port', None)
    vo_psql_pg_user = kwargs.get('vo_psql_pg_user', None)
    vo_psql_pg_password = kwargs.get('vo_psql_pg_password', None)
    vo_psql_pg_db = kwargs.get('vo_psql_pg_db', None)
    product_gallery_url = kwargs.get('product_gallery_url', None)
    result_query = run_ivoa_query_from_product_gallery(parsed_query_obj,
                                                      vo_psql_pg_host=vo_psql_pg_host,
                                                      vo_psql_pg_port=vo_psql_pg_port,
                                                      vo_psql_pg_user=vo_psql_pg_user,
                                                      vo_psql_pg_password=vo_psql_pg_password,
                                                      vo_psql_pg_db=vo_psql_pg_db,
                                                      product_gallery_url=product_gallery_url)
    return result_query


def map_psql_type_to_vo_datatype(type_code):
    type_db = psycopg2.extensions.string_types[type_code]
    print(f"type db is: {type_db.name}")
    if type_db.name == 'LONGINTEGER':
        return 'int'
    elif type_db.name == 'STRING':
        return 'char'
    elif type_db.name == 'FLOAT':
        return 'double'
    return 'char'


def map_psql_null_to_vo_default_value(datatype):
    if datatype == 'char':
        return ""
    elif datatype in 'int':
        return -1
    elif datatype in 'double':
        return np.nan

def run_ivoa_query_from_product_gallery(parsed_query_obj,
                                        vo_psql_pg_host,
                                        vo_psql_pg_port,
                                        vo_psql_pg_user,
                                        vo_psql_pg_password,
                                        vo_psql_pg_db,
                                        product_gallery_url=None
                                        ):
    result_list = []
    connection = None
    # Create a new VOTable file with one resource and one table
    votable = VOTableFile()
    resource = Resource()
    votable.resources.append(resource)
    table = Table(votable)
    resource.tables.append(table)

    # Define fields
    table.fields.extend([
        Field(votable, name="nid", datatype="int", arraysize="*"),
        Field(votable, name="title", datatype="char", arraysize="*"),
        Field(votable, name="RA", datatype="double", arraysize="*"),
        Field(votable, name="DEC", datatype="double", arraysize="*"),
        Field(votable, name="e1_kev", datatype="double", arraysize="*"),
        Field(votable, name="e2_kev", datatype="double", arraysize="*"),
        Field(votable, name="path_alias", datatype="char", arraysize="*"),
        Field(votable, name="path", datatype="char", arraysize="*"),
        Field(votable, name="product_id", datatype="char", arraysize="*"),
        Field(votable, name="time_bin", datatype="double", arraysize="*"),
        Field(votable, name="instrument_name", datatype="char", arraysize="*"),
        Field(votable, name="instrument_description_value", datatype="char", arraysize="*"),
        Field(votable, name="product_type_name", datatype="char", arraysize="*"),
        Field(votable, name="description__value", datatype="char", arraysize="*"),
        Field(votable, name="rev1", datatype="int", arraysize="*"),
        Field(votable, name="rev2", datatype="int", arraysize="*"),
        Field(votable, name="timerange", datatype="char", arraysize="*"),
        Field(votable, name="timerange_end", datatype="char", arraysize="*"),
        Field(votable, name="proposal_id", datatype="char", arraysize="*"),
        Field(votable, name="sources", datatype="char", arraysize="*"),
        Field(votable, name="file_target_id", datatype="char", arraysize="*"),
        Field(votable, name="file_name", datatype="char", arraysize="*"),
        Field(votable, name="file_uri", datatype="char", arraysize="*"),
        Field(votable, name="image_name", datatype="char", arraysize="*"),
        Field(votable, name="image_uri", datatype="char", arraysize="*"),
    ])

    for f in table.fields:
        if f.datatype == 'char':
            f.values.null = ""
        elif f.datatype in 'int':
            f.values.null = -1
        elif f.datatype in 'double':
            f.values.null = np.nan

    # Create a new VOTable file with one resource and one table
    d_votable = VOTableFile()
    d_resource = Resource()
    d_votable.resources.append(d_resource)
    d_table = Table(d_votable)
    d_resource.tables.append(d_table)

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
                table.create_arrays(len(data))
                # d_table.create_arrays(len(data))

                # for r_index, row in enumerate(data):
                #     datatype = map_psql_type_to_vo_datatype(description.type_code)
                #     default_no_value = map_psql_null_to_vo_default_value(datatype)
                #     # type_db = psycopg2.extensions.string_types[description.type_code]
                #     # print(f"type db is: {type_db.name}")
                #     f = Field(votable, ID=description.name, name=description.name, datatype=datatype, arraysize="*")
                    # f.values.null = default_no_value
                    # d_table.fields.append(f)
                for r_index, row in enumerate(data):
                    table_row = list(row)
                    d_table_entry = [""] * len(table_row)
                    table_entry = [""] * len(table.fields)
                    for v_index, value in enumerate(table_row):
                        description = cursor.description[v_index]
                        datatype = map_psql_type_to_vo_datatype(description.type_code)
                        default_no_value = map_psql_null_to_vo_default_value(datatype)
                        if r_index == 0:
                            # type_db = psycopg2.extensions.string_types[description.type_code]
                            # print(f"type db is: {type_db.name}")
                            f = Field(votable, ID=description.name, name=description.name, datatype=datatype, arraysize="*")
                            f.values.null = default_no_value
                            d_table.fields.append(f)
                        if value is None:
                            d_table_entry[v_index] = default_no_value
                        else:
                            d_table_entry[v_index] = value

                        if description.name == 'nid':
                            table_entry[0] = value
                            if value is None:
                                table_entry[0] = -1
                        if description.name == 'title':
                            table_entry[1] = value
                            if value is None:
                                table_entry[1] = ""
                        if description.name == 'ra':
                            table_entry[2] = value
                            if value is None:
                                table_entry[2] = np.nan
                        if description.name == 'dec':
                            table_entry[3] = value
                            if value is None:
                                table_entry[3] = np.nan
                        if description.name == 'e1_kev':
                            table_entry[4] = value
                            if value is None:
                                table_entry[4] = np.nan
                        if description.name == 'e2_kev':
                            table_entry[5] = value
                            if value is None:
                                table_entry[5] = np.nan
                        if description.name == 'path_alias':
                            table_entry[6] = value
                            if value is None:
                                table_entry[6] = ""
                        if description.name == 'path':
                            table_entry[7] = value
                            if value is None:
                                table_entry[7] = ""
                        if description.name == 'product_id':
                            table_entry[8] = value
                            if value is None:
                                table_entry[8] = ""
                        if description.name == 'time_bin':
                            table_entry[9] = value
                            if value is None:
                                table_entry[9] = np.nan
                        if description.name == 'instrument_name':
                            table_entry[10] = value
                            if value is None:
                                table_entry[10] = ""
                        if description.name == 'instrument_description_value':
                            table_entry[11] = value
                            if value is None:
                                table_entry[11] = ""
                        if description.name == 'product_type_name':
                            table_entry[12] = value
                            if value is None:
                                table_entry[12] = ""
                        if description.name == 'description__value':
                            table_entry[13] = value
                            if value is None:
                                table_entry[13] = ""
                        if description.name == 'rev1':
                            table_entry[14] = value
                            if value is None:
                                table_entry[14] = -1
                        if description.name == 'rev2':
                            table_entry[15] = value
                            if value is None:
                                table_entry[15] = -1
                        if description.name == 'timerange':
                            table_entry[16] = value
                            if value is None:
                                table_entry[16] = ""
                        if description.name == 'timerange_end':
                            table_entry[17] = value
                            if value is None:
                                table_entry[17] = ""
                        if description.name == 'proposal_id':
                            table_entry[18] = value
                            if value is None:
                                table_entry[18] = ""
                        if description.name == 'sources':
                            table_entry[19] = value
                            if value is None:
                                table_entry[19] = ""
                        if description.name == 'file_target_id':
                            table_entry[20] = value
                            if value is None:
                                table_entry[20] = ""
                        if description.name == 'file_name':
                            table_entry[21] = value
                            if value is None:
                                table_entry[21] = ""
                        if description.name == 'file_uri':
                            table_entry[22] = value
                            if value is None:
                                table_entry[22] = ""
                        if description.name == 'image_name':
                            table_entry[23] = value
                            if value is None:
                                table_entry[23] = ""
                        if description.name == 'image_uri':
                            table_entry[24] = value
                            if value is None:
                                table_entry[24] = ""

                        if product_gallery_url is not None:
                            if description.name in {'file_uri', 'file_name', 'image_name', 'image_uri'} and value is not None and isinstance(value, str):
                                value_list = [v.strip() for v in value.split(',')]
                                if description.name == 'file_uri' or description.name == 'image_uri':
                                    for file_id, file_name in enumerate(value_list):
                                        value_list[file_id] = os.path.join(product_gallery_url, 'sites/default/files/', file_name.strip())
                                table_row[v_index] = value_list
                            if description.name in {'path', 'path_alias'} and value is not None and isinstance(value, str):
                                if value.startswith('/'):
                                    value = value[1:]
                                table_row[v_index] = os.path.join(product_gallery_url, value)
                    result_list.append(table_row)
                    table.array[r_index] = tuple(table_entry)
                    if r_index == 0:
                        d_table.create_arrays(len(data))
                    d_table.array[r_index] = tuple(d_table_entry)

    except (Exception, DatabaseError) as e:
        logger.error(f"Error when querying to the Postgresql server: {str(e)}")
        raise e

    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            logger.info('Database connection closed')

    votable.to_xml('output.xml')
    d_votable.to_xml('d_output.xml')
    with open('output.xml', 'r') as f:
        votable_xml_output = f.read()

    return votable_xml_output
