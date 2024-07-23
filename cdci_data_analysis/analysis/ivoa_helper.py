import antlr4
from queryparser.adql import ADQLQueryTranslator
from queryparser.postgresql import PostgreSQLQueryProcessor
from queryparser.postgresql.PostgreSQLParser import PostgreSQLParser
from queryparser.exceptions import QuerySyntaxError

from queryparser.postgresql.PostgreSQLParserListener import PostgreSQLParserListener

import sqlparse
import json

from ..app_logging import app_logging

from ..analysis import drupal_helper

logger = app_logging.getLogger('ivoa_helper')


class WhereClauseListener(PostgreSQLParserListener):
    def __init__(self):
        self.where_clause = None

    def enterWhere_clause(self, ctx):
        conditions = self.extract_elements(ctx)
        self.where_clause = conditions

    def extract_elements(self, node):
        elements = []
        for child in node.getChildren():
            if isinstance(child, PostgreSQLParser.ExpressionContext):
                elements.extend(self.extract_elements(child))
            else:
                elements.append(child.getText())
        return elements


def parse_adql_query(query):
    try:
        # queryparser
        adt = ADQLQueryTranslator(query)
        qp = PostgreSQLQueryProcessor()
        where_listener = WhereClauseListener()
        qp.set_query(adt.to_postgresql())
        qp.process_query()

        inpt = antlr4.InputStream(query)
        lexer = qp.lexer(inpt)
        stream = antlr4.CommonTokenStream(lexer)
        parser = qp.parser(stream)
        tree = parser.query()
        qp.walker.walk(where_listener, tree)

        output_obj = dict(
            columns=qp.display_columns,
            tables=qp.tables,
            rest=qp,
            where_clause=where_listener.where_clause
        )

        # sqlparse
        parsed_query_obj = sqlparse.parse(query)[0]

        for t in parsed_query_obj.tokens:
            if isinstance(t, sqlparse.sql.Where):
                output_obj['where_token'] = t

    except QuerySyntaxError as qe:
        logger.error(f'Error parsing ADQL query: {qe}')
        output_obj = dict(
            where_clause=None,
            tables=None,
            columns=None,
            rest=None
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
