import os.path
import psycopg2.extensions

import numpy as np
import xml.etree.ElementTree as ET
import astropy.units as u

from queryparser.adql import ADQLQueryTranslator
from queryparser.exceptions import QuerySyntaxError
from psycopg2 import connect, DatabaseError

from ..app_logging import app_logging
from ..analysis.exceptions import RequestNotUnderstood

from astropy.io.votable.tree import VOTableFile, Resource, Field, Table
from astropy import constants

logger = app_logging.getLogger('ivoa_helper')


def map_psql_type_code_to_vo_datatype(type_code):
    type_db = psycopg2.extensions.string_types[type_code]
    return map_psql_type_to_vo_datatype(type_db.name)


def map_psql_type_to_vo_datatype(type_db):
    if type_db.upper() == 'LONGINTEGER' or type_db.upper() == 'BIGINT':
        return 'int'
    elif type_db.upper() == 'STRING':
        return 'char'
    elif type_db.upper() == 'FLOAT' or type_db.upper() == 'DOUBLE PRECISION':
        return 'double'
    elif type_db.upper() == 'BOOLEAN':
        return 'boolean'
    elif type_db.upper() == 'BYTEA':
        return 'unsignedByte'
    return 'char'


def map_vo_type_to_vo_default_value(datatype):
    if datatype == 'char':
        return ""
    elif datatype in 'int':
        return -1
    elif datatype in 'double':
        return np.nan
    return ""


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


def run_metadata_query(**kwargs):
    logger.info('Performing metadata query on the product_gallery')
    vo_psql_pg_host = kwargs.get('vo_psql_pg_host', None)
    vo_psql_pg_port = kwargs.get('vo_psql_pg_port', None)
    vo_psql_pg_user = kwargs.get('vo_psql_pg_user', None)
    vo_psql_pg_password = kwargs.get('vo_psql_pg_password', None)
    vo_psql_pg_db = kwargs.get('vo_psql_pg_db', None)
    # following https://wiki.ivoa.net/internal/IVOA/VODataService/VODataService-v1.1wd.html
    xml_output_root = ET.Element('vod:tableset', {
        'xmlns:vod': 'http://www.ivoa.net/xml/VODataService/v1.1',
        'xsi:type': 'vod:TableSet',
        'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        'xsi:schemaLocation': 'http://www.ivoa.net/xml/VODataService/v1.1 http://esa.int/xml/EsaTapPlus https://gea.esac.esa.int/tap-server/xml/esaTapPlusAttributes.xsd'
    })

    extract_metadata_from_product_gallery(xml_output_root=xml_output_root,
                                          vo_psql_pg_host=vo_psql_pg_host,
                                          vo_psql_pg_port=vo_psql_pg_port,
                                          vo_psql_pg_user=vo_psql_pg_user,
                                          vo_psql_pg_password=vo_psql_pg_password,
                                          vo_psql_pg_db=vo_psql_pg_db)

    return ET.tostring(xml_output_root, encoding='unicode')

def run_adql_query(query, **kwargs):
    parsed_query_obj = parse_adql_query(query)

    psql_query = parsed_query_obj['psql_query']
    logger.info('Performing query on the product_gallery')
    vo_psql_pg_host = kwargs.get('vo_psql_pg_host', None)
    vo_psql_pg_port = kwargs.get('vo_psql_pg_port', None)
    vo_psql_pg_user = kwargs.get('vo_psql_pg_user', None)
    vo_psql_pg_password = kwargs.get('vo_psql_pg_password', None)
    vo_psql_pg_db = kwargs.get('vo_psql_pg_db', None)
    product_gallery_url = kwargs.get('product_gallery_url', None)
    result_query = run_query_from_product_gallery(psql_query,
                                                  vo_psql_pg_host=vo_psql_pg_host,
                                                  vo_psql_pg_port=vo_psql_pg_port,
                                                  vo_psql_pg_user=vo_psql_pg_user,
                                                  vo_psql_pg_password=vo_psql_pg_password,
                                                  vo_psql_pg_db=vo_psql_pg_db,
                                                  product_gallery_url=product_gallery_url)
    return result_query


def run_query_from_product_gallery(psql_query,
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

    columns_to_exclude = ['nid']
    indexes_to_exclude = []

    try:
        with connect(
            host=vo_psql_pg_host,
            port=vo_psql_pg_port,
            database=vo_psql_pg_db,
            user=vo_psql_pg_user,
            password=vo_psql_pg_password
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(psql_query)
                data = cursor.fetchall()
                # loop over the description of the data result to define the fields of the output VOTable
                for column_index, column in enumerate(cursor.description):
                    # purely drupal related, not related within the context of TAP
                    if column.name in columns_to_exclude:
                        indexes_to_exclude.append(column_index)
                        continue
                    datatype = map_psql_type_code_to_vo_datatype(column.type_code)
                    default_no_value = map_vo_type_to_vo_default_value(datatype)
                    f = Field(votable, ID=column.name, name=column.name, datatype=datatype, arraysize="*")
                    # TODO find a way to extract the column description from the DB
                    f.description = ''
                    f.values.null = default_no_value
                    table.fields.append(f)
                table.create_arrays(len(data))
                for r_index, row in enumerate(data):
                    table_row = list(row)
                    table_entry = [""] * len(row)
                    # for each column of a table_row
                    for v_index, value in enumerate(table_row):
                        # Get the column description and its corresponding datatype and default value in case of null in the DB
                        # then create the field in the VOTable obj
                        description = cursor.description[v_index]
                        # purely drupal related, not related within the context of TAP
                        if description.name in columns_to_exclude:
                            continue
                        table_entry[v_index] = value

                        if product_gallery_url is not None:
                            if description.name in {'file_uri', 'file_name', 'image_name', 'image_uri', 'access_url'} and value is not None and isinstance(value, str):
                                value_list = [v.strip() for v in value.split(',')]
                                if description.name == 'file_uri' or description.name == 'image_uri' or description.name == 'access_url':
                                    for file_id, file_name in enumerate(value_list):
                                        value_list[file_id] = os.path.join(product_gallery_url, 'sites/default/files/', file_name.strip())
                                table_entry[v_index] = ",".join(value_list)
                            if description.name in {'path', 'path_alias', 'product_path'} and value is not None and isinstance(value, str):
                                if value.startswith('/'):
                                    value = value[1:]
                                table_entry[v_index] = os.path.join(product_gallery_url, value)

                        datatype = map_psql_type_code_to_vo_datatype(description.type_code)
                        default_no_value = map_vo_type_to_vo_default_value(datatype)
                        if value is None:
                            table_entry[v_index] = default_no_value

                    # remove entry based on the index to excludes
                    for index in indexes_to_exclude:
                        del table_entry[index]

                    table.array[r_index] = tuple(table_entry)

    except (Exception, DatabaseError) as e:
        logger.error(f"Error when querying to the Postgresql server: {str(e)}")
        raise e

    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            logger.info('Database connection closed')

    try:
        votable.to_xml('output.xml')
        with open('output.xml', 'r') as f:
            votable_xml_output = f.read()
    except Exception as e:
        logger.error(f"Error when writing the VOTable to XML: {str(e)}")
        raise e
    finally:
        if os.path.exists('output.xml'):
            os.remove('output.xml')

    return votable_xml_output

def extract_metadata_from_product_gallery(xml_output_root,
                                          vo_psql_pg_host,
                                          vo_psql_pg_port,
                                          vo_psql_pg_user,
                                          vo_psql_pg_password,
                                          vo_psql_pg_db,
                                          ):
    # gallery tables query
    tables_gallery_query = ("SELECT t.table_schema AS table_schema, t.table_name AS table_name, t.table_type AS table_type, "
                            "string_agg(d.description, ' ') AS table_description "
                            "FROM information_schema.tables t LEFT JOIN pg_catalog.pg_description d "
                            "ON d.objoid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = t.table_name AND relkind = 'r' LIMIT 1) "
                            "WHERE d.objsubid = 0 AND table_schema != 'pg_catalog' AND table_schema != 'information_schema' "
                            "GROUP BY t.table_schema, t.table_name, t.table_type ORDER BY t.table_schema, t.table_name;")

    columns_table_gallery_query = ("SELECT c.column_name, c.data_type, c.column_default, "
                                   "COL_DESCRIPTION(CONCAT(c.table_schema, '.', c.table_name)::regclass, ordinal_position) as description "
                                   "FROM information_schema.columns as c "
                                   "JOIN information_schema.tables as t "
                                   "ON t.table_catalog = c.table_catalog "
                                   "AND t.table_schema = c.table_schema "
                                   "AND t.table_name = c.table_name "
                                   "WHERE c.table_name = '{table_name}' "
                                   "AND c.table_schema = '{schema_name}' "
                                   "ORDER BY c.column_name;")

    columns_to_exclude = {'ivoa.obscore': 'nid'}

    try:
        with connect(
            host=vo_psql_pg_host,
            port=vo_psql_pg_port,
            database=vo_psql_pg_db,
            user=vo_psql_pg_user,
            password=vo_psql_pg_password
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(tables_gallery_query)
                data = cursor.fetchall()
                if len(data) > 0:
                    # for each row in the query result
                    for r_index, row in enumerate(data):
                        table_row = list(row)
                        schema_elem_name = None
                        table_elem_name = None
                        description_elem_name = None
                        table_type_name = None
                        # for each column in the row, get the column description and its corresponding value, to create a table element in the xml output, and the relative schema if needed
                        for v_index, value in enumerate(table_row):
                            description = cursor.description[v_index]
                            if description.name == 'table_schema':
                                schema_elem_name = value
                            if description.name == 'table_name':
                                table_elem_name = value
                            if description.name == 'table_description':
                                description_elem_name = value
                            if description.name == 'table_type':
                                table_type_name = value

                        if schema_elem_name is not None:
                            schema_elem = get_schema_element(xml_output_root, schema_elem_name)
                            if schema_elem is None:
                                schema_elem = ET.SubElement(xml_output_root, 'schema')
                                ET.SubElement(schema_elem, 'name').text = schema_elem_name
                                ET.SubElement(schema_elem, 'description').text = 'description'
                            if table_elem_name is not None:
                                table_elem = ET.SubElement(schema_elem, 'table')
                                if table_type_name is not None:
                                    table_elem.set('type', '_'.join(table_type_name.lower().split()))
                                ET.SubElement(table_elem, 'name').text = table_elem_name
                                if description_elem_name is not None:
                                    ET.SubElement(table_elem, 'description').text = description_elem_name
                                else:
                                    ET.SubElement(table_elem, 'description').text = 'description'

                            formatted_columns_table_gallery_query = columns_table_gallery_query.format(table_name=table_elem_name, schema_name=schema_elem_name)
                            with connection.cursor() as column_cursor:
                                column_cursor.execute(formatted_columns_table_gallery_query)
                                columns_table_data = column_cursor.fetchall()
                                for c_t_index, c_t_row in enumerate(columns_table_data):
                                    if columns_to_exclude.get(schema_elem_name + '.' + table_elem_name, None) is not None and columns_to_exclude[schema_elem_name + '.' + table_elem_name] == c_t_row[0]:
                                        continue
                                    # considering the query
                                    # "SELECT c.column_name, c.data_type, c.column_default, "
                                    # "COL_DESCRIPTION(CONCAT(c.table_schema, '.', c.table_name)::regclass, ordinal_position) as description "
                                    column_elem_name = c_t_row[0]
                                    column_datatype = c_t_row[1]
                                    column_default = c_t_row[2]
                                    column_description = c_t_row[3]
                                    column_elem = ET.SubElement(table_elem, 'column')
                                    ET.SubElement(column_elem, 'name').text = column_elem_name
                                    if column_description is not None:
                                        ET.SubElement(column_elem, 'description').text = column_description
                                    else:
                                        ET.SubElement(column_elem, 'description').text = 'description'
                                    if column_datatype is not None:
                                        vo_table_type = map_psql_type_to_vo_datatype(column_datatype)
                                        data_type_elem = ET.SubElement(column_elem, 'dataType')
                                        table_elem.set('type', '_'.join(table_type_name.lower().split()))
                                        data_type_elem.text = vo_table_type
                                        data_type_elem.set('xsi:type', 'vod:VOTableType')

    except (Exception, DatabaseError) as e:
        logger.error(f"Error when querying to the Postgresql server: {str(e)}")
        raise e

    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            logger.info('Database connection closed')


def get_schema_element(table_set_element, schema_name):
    for schema_elem in table_set_element.findall('schema'):
        name_elem = schema_elem.find('name')
        if name_elem is not None and name_elem.text == schema_name:
            return schema_elem
    return None

def kev_to_meters(energy_keV):
    energy = energy_keV * u.keV
    wavelength = (constants.h * constants.c / energy).to(u.m).value
    return wavelength