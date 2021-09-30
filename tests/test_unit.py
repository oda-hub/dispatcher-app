import pytest

from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.queries import (
    ProductQuery,
    SourceQuery,
    InstrumentQuery,
    Float,
    Name,
)


@pytest.mark.parametrize("add_duplicate", [True, False])
def test_repeating_parameters(add_duplicate):
    src_query = SourceQuery("src_query")

    instr_query = InstrumentQuery(
        name="empty_async_instrument_query",
        input_prod_list_name="scw_list",
    )

    p1 = Float(
        value=10.0,
        name="duplicate-name",
        units="W",
    )
    p2 = Name(value="default-name", name="duplicate-name")

    if add_duplicate:
        parameters_list = [p1, p2]
    else:
        parameters_list = [p1]

    product_query = ProductQuery("test_product_query", parameters_list=parameters_list)

    query_dictionary = {"numerical": "numerical_parameters_dummy_query"}

    instrument = Instrument(
        "empty-async",
        src_query=src_query,
        instrumet_query=instr_query,
        product_queries_list=[product_query],
        query_dictionary=query_dictionary,
        data_server_query_class=None,
    )

    # TODO: this is current behavior. This is hardly desirable. It should be fixed eventually.
    if add_duplicate:
        assert instrument.get_par_by_name("duplicate-name") == p2
        assert instrument.get_par_by_name("duplicate-name") != p1
        assert [p["field name"] for p in product_query.par_dictionary_list] == [
            "duplicate-name",
            "duplicate-name",
        ]
    else:
        assert instrument.get_par_by_name("duplicate-name") == p1
