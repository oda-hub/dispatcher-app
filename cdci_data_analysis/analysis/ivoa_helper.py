from queryparser.adql import ADQLQueryTranslator
from queryparser.postgresql import PostgreSQLQueryProcessor
from queryparser.exceptions import QuerySyntaxError

from ..app_logging import app_logging


logger = app_logging.getLogger('ivoa_helper')


def parse_adql_query(query):
    try:
        adt = ADQLQueryTranslator(query)
        qp = PostgreSQLQueryProcessor()
        qp.set_query(adt.to_postgresql())
        qp.process_query()

        output_obj = dict(
            columns = qp.columns,
            display_columns = qp.display_columns,
            tables = qp.tables,
            rest = qp
        )
    except QuerySyntaxError as qe:
        logger.error(f'Error parsing ADQL query: {qe}')
        output_obj = dict(
            columns = [],
            display_columns = [],
            tables = [],
        )
    return output_obj
