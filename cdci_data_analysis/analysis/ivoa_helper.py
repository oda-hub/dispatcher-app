from queryparser.adql import ADQLQueryTranslator
from queryparser.postgresql import PostgreSQLQueryProcessor

from ..app_logging import app_logging


logger = app_logging.getLogger('ivoa_helper')


def parse_adql_query(query):
    adt = ADQLQueryTranslator(query)
    qp = PostgreSQLQueryProcessor()
    qp.set_query(adt.to_postgresql())
    qp.process_query()

    output_obj = dict(
        columns = qp.columns,
        display_columns = qp.display_columns,
    )

    return output_obj
