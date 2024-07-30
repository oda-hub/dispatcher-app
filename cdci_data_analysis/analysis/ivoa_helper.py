import antlr4
from queryparser.adql import ADQLQueryTranslator
from queryparser.postgresql import PostgreSQLQueryProcessor
from queryparser.postgresql.PostgreSQLParser import PostgreSQLParser
from queryparser.mysql import MySQLQueryProcessor
from queryparser.exceptions import QuerySyntaxError
from collections import deque

from mysql.connector import connect, Error

from queryparser.postgresql.PostgreSQLParserListener import PostgreSQLParserListener

from ..app_logging import app_logging

from ..analysis import drupal_helper

logger = app_logging.getLogger('ivoa_helper')


class WhereClauseListener(PostgreSQLParserListener):
    def __init__(self):
        self.where_clause = None

    def enterWhere_clause(self, ctx):
        conditions = self.analyze_expressions(ctx)
        self.where_clause = conditions

    def analyze_expressions(self, node):
        output_obj = dict()
        for child in node.getChildren():
            if isinstance(child, PostgreSQLParser.ExpressionContext):
                output_obj['conditions'] = self.extract_conditions_from_hierarchy(child)
        return output_obj

    def extract_conditions_from_hierarchy(self, context, conditions=None):
        if conditions is None:
            conditions = []

        queue = deque([(context, 0)])
        column_level = relation_level = number_literal_level = 0
        while queue:
            context, level = queue.popleft()

            if isinstance(context, antlr4.ParserRuleContext):
                print(f"{'  ' * level} - {type(context).__name__}, level: {level}")
                if isinstance(context, PostgreSQLParser.Bool_primaryContext):
                    print("Bool_primaryContext reached")
                    conditions.append({})
                elif isinstance(context, PostgreSQLParser.Column_nameContext):
                    print("Column_nameContext reached")
                    conditions[column_level]['column'] = context.getText()
                    column_level += 1
                elif isinstance(context, PostgreSQLParser.Relational_opContext):
                    print("Relational_opContext reached")
                    conditions[relation_level]['operator'] = context.getText()
                    relation_level += 1
                elif isinstance(context, PostgreSQLParser.Number_literalContext):
                    print("Number_literalContext reached")
                    conditions[number_literal_level]['value'] = context.getText()
                    number_literal_level += 1
                for child in context.children:
                    print(
                        f"{'  ' * level} - {type(child).__name__}, level: {level}, childGetText: {child.getText()}, conditions size: {len(conditions)}")
                    queue.append((child, level + 1))

        return conditions


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
            # where_clause=where_listener.where_clause
        )

    except QuerySyntaxError as qe:
        logger.error(f'Error parsing ADQL query: {qe}')
        output_obj = dict(
            # where_clause=None,
            tables=None,
            columns=None,
            rest=None,
            mysql_query=None
        )
    return output_obj


def run_ivoa_query(query, sentry_dsn=None, **kwargs):
    result_list = []
    parsed_query_obj = parse_adql_query(query)

    # tables = parsed_query_obj.get('tables', [])
    # if len(tables) == 1 and tables[0] == 'product_gallery':
    logger.info('Performing query on the product_gallery')
        # product_gallery_url = kwargs.get('product_gallery_url', None)
        # gallery_jwt_token = kwargs.get('gallery_jwt_token', None)
        # if product_gallery_url and gallery_jwt_token:
    result_list = run_ivoa_query_from_product_gallery(parsed_query_obj)
    return result_list


def run_ivoa_query_from_product_gallery(parsed_query_obj):
    result_list = []

    try:
        with connect(
                # TODO: Add the connection details reading from the config file
                host="",
                user="",
                password="",
                database=""
        ) as connection:
            print(connection)

            create_db_query = parsed_query_obj.get('mysql_query')
            with connection.cursor() as cursor:
                cursor.execute(create_db_query)
                for db in cursor:
                    print(db)

    except Error as e:
        print(e)

    return result_list
