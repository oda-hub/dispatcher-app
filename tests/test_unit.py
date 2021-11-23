import ast
from _pytest import outcomes

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

        assert p_spectral_boundary.get_value_in_default_format() == p_spectral_boundary.value
        assert p_spectral_boundary.get_value_in_default_format() == float(10.)
        assert type(p_spectral_boundary.value) == float



def test_parameter_normalization_no_units():
    for parameter_type, input_value, outcome in [
            (Float, 25, 25.0),
            (Float, 25., 25.0),
            (Float, 25.64547871216879451687311, 25.64547871216879451687311),
            (Float, "25", 25.0),
            (Float, "25.", 25.0),
            (Float, "25.64547871216879451687311", 25.64547871216879451687311),
            (Float, "2.5e1", 25.0),
            (Float, "aaaa", RuntimeError),
            (Integer, 25, 25),
            (Integer, 25., RuntimeError),
            (Integer, 25.64547871216879451687311, RuntimeError),
            (Integer, "25", 25),
            (Integer, "25.", RuntimeError),
            (Integer, "25.64547871216879451687311", RuntimeError),
            (Integer, "aaaa", RuntimeError)
        ]:

        def constructor():
            return parameter_type(value=input_value, name="my-parameter-name")

        if isinstance(outcome, type) and issubclass(outcome, Exception):
            with pytest.raises(outcome):
                constructor()
        else:
            # this also sets the default value
            parameter = constructor()

            # this is redundant
            assert parameter.get_value_in_default_format() == parameter.value

            assert parameter.value == outcome
            assert type(parameter.value) == type(outcome)

            # setting value during request
            
            assert parameter.set_par(input_value) == outcome
            assert parameter.value == outcome
            assert type(parameter.value) == type(outcome)
            
