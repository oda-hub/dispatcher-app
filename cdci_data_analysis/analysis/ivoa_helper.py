from queryparser.exceptions import QuerySyntaxError

import sqlparse
import json

from ..app_logging import app_logging

from ..analysis import drupal_helper


logger = app_logging.getLogger('ivoa_helper')


def parse_adql_query(query):
    try:
        output_obj = dict()
        parsed_query_obj = sqlparse.parse(query)[0]
        from_seen = False
        for t in parsed_query_obj.tokens:
            if isinstance(t, sqlparse.sql.Where):
                output_obj['where_token'] = t
            if from_seen:
                    if isinstance(t, sqlparse.sql.Identifier):
                        output_obj['tables'] = [t.get_name()]
                    elif isinstance(t, sqlparse.sql.IdentifierList):
                        output_obj['tables'] = [x.get_name() for x in t.get_identifiers()]
            if t.is_keyword and t.ttype is sqlparse.tokens.Keyword and t.value.upper() == 'FROM':
                from_seen = True

    except QuerySyntaxError as qe:
        logger.error(f'Error parsing ADQL query: {qe}')
        output_obj = dict(
            where_token = None,
            tables = None
        )
    return output_obj


def run_ivoa_query(query, sentry_dsn=None, **kwargs):
    result_list = []
    parsed_query_obj = parse_adql_query(query)

    tables = parsed_query_obj.get('tables', [])
    if len(tables) == 1 and tables[0] == 'product_gallery':
        logger.info('Query is a product_gallery query')
        product_gallery_url = kwargs.get('product_gallery_url', None)
        gallery_jwt_token = kwargs.get('gallery_jwt_token', None)
        if product_gallery_url and gallery_jwt_token:
            result_list = run_ivoa_query_from_product_gallery(
                product_gallery_url,
                gallery_jwt_token,
                sentry_dsn=sentry_dsn,
                **kwargs
            )
    return result_list


def run_ivoa_query_from_product_gallery(product_gallery_url,
                                        gallery_jwt_token,
                                        sentry_dsn=None,
                                        **kwargs):
    output_get = drupal_helper.get_data_product_list_by_source_name_with_conditions(
        product_gallery_url=product_gallery_url,
        gallery_jwt_token=gallery_jwt_token,
        sentry_dsn=sentry_dsn,
        **kwargs)

    output_list = json.dumps(output_get)

    return output_list