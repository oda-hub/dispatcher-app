import pytest

from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.parameters import Integer, SpectralBoundary
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


@pytest.mark.parametrize("value",  [25, 25., 25.64547871216879451687311211245117852145229614585985498212321, "aaaa"])
def test_float_defaults(value):
    if type(value) == str:
        with pytest.raises(RuntimeError):
            Float(
                value=value,
                name="p_float"
            )
    else:
        p_float = Float(
            value=value,
            name="p_float"
        )

        assert p_float.get_value_in_default_format(p_float.value) == p_float.value
        # assign an int value, that then should be converted to float
        p_float.value = value
        assert p_float.get_value_in_default_format(p_float.value) == float(value)
        assert type(p_float.value) == float


@pytest.mark.parametrize("e_units", ['eV', 'W', '', None])
def test_spectral_boundaries_defaults(e_units):
    # test with a not allowed unit
    if e_units == 'W':
        with pytest.raises(RuntimeError):
            SpectralBoundary(
                value=10.,
                name="p_spectral_boundary",
                E_units=e_units,
            )
    else:
        p_spectral_boundary = SpectralBoundary(
            value=10.,
            name="p_spectral_boundary",
            E_units=e_units,
        )

        assert p_spectral_boundary.get_value_in_default_format(p_spectral_boundary.value) == p_spectral_boundary.value
        assert p_spectral_boundary.get_value_in_default_format(p_spectral_boundary.value) == float(10.)
        assert type(p_spectral_boundary.value) == float


@pytest.mark.parametrize("value",  [25, 25., 25.64547871216879451687311211245117852145229614585985498212321, "aaaa"])
def test_integer_defaults(value):
    if type(value) == str:
        with pytest.raises(RuntimeError):
            Integer(
                value=value,
                name="p_integer"
            )
    else:
        p_integer = Integer(
            value=value,
            name="p_integer"
        )
        assert p_integer.value == int(value)
        assert p_integer.get_value_in_default_format(p_integer.value) == int(value)
        assert type(p_integer.value) == int
