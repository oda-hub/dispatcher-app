import ast

import pytest

from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.queries import (
    ProductQuery,
    SourceQuery,
    InstrumentQuery,
    Float,
    Integer,
    Energy,
    SpectralBoundary,
    Name,
    Time,
    TimeDelta,
    ParameterRange,
    ParameterTuple,
    Angle,
    InputProdList
)

import numpy as np


@pytest.mark.fast
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

@pytest.mark.fast
def test_input_prod_list():
    for parameter_type, input_value, format_args, outcome in [
        (InputProdList, [1, 2, 3], {'_format': 'names_list'}, [1, 2, 3]),
        (InputProdList, [1, 2, 3], {}, [1, 2, 3]),
        (InputProdList, [1, 2, 3], {'_format': 'things_list'}, RuntimeError),
        (InputProdList, {}, {}, RuntimeError),
        (InputProdList, 1, {'_format': 'names_list'}, ['1']),
        (InputProdList, 'aaa', {'_format': 'names_list'}, ['aaa']),
        (InputProdList, '1 2 34', {'_format': 'names_list'}, ['1 2 34']),
        (InputProdList, [1, '2', 3], {'_format': 'names_list'}, [1, '2', 3]),
    ]:
        def constructor():
            return parameter_type(value=input_value,
                                  name="my-parameter-name",
                                  **format_args
                                  )
        if isinstance(outcome, type) and issubclass(outcome, Exception):
            with pytest.raises(outcome):
                constructor()
        else:
            # this also sets the default value
            parameter = constructor()
            # check stuff on the inputProdList
            assert parameter.value == outcome

@pytest.mark.fast
def test_energy_defaults():
    for parameter_type, input_value, outcome, e_units, expected_type in [
        (Energy, 10., 10., 'keV', float),
        (SpectralBoundary, 10., 10., 'keV', float),
        (SpectralBoundary, 10, 10., 'keV', float),
        (SpectralBoundary, 10., 10., 'eV', float),
        (SpectralBoundary, 10., RuntimeError, 'W', None),
        (SpectralBoundary, 'ssss', RuntimeError, None, None),
    ]:
        def constructor():
            return parameter_type(value=input_value,
                                  name="p_spectral_boundary",
                                  E_units=e_units
                                  )
        if isinstance(outcome, type) and issubclass(outcome, Exception):
            with pytest.raises(outcome):
                constructor()
        else:
            p_spectral_boundary = constructor()

            assert p_spectral_boundary.get_value_in_default_units() == p_spectral_boundary.value
            assert p_spectral_boundary.get_value_in_default_units() == outcome
            assert type(p_spectral_boundary.value) == expected_type

@pytest.mark.fast
def test_angle_parameter():
    for parameter_type, input_value, format_args, outcome, outcome_default_format in [
        (Angle, -29.74516667, {'units': 'deg'}, -29.74516667, -29.74516667),
        (Angle, -29.74516667, {'units': 'deg', 'default_units': 'deg'}, -29.74516667, -29.74516667),
        (Angle, 3, {'units': 'arcmin', 'default_units': 'deg'}, 3, 0.05),
        (Angle, 3, {'units': 'arcmin', 'default_units': 'arcmin'}, 3, 3),
        (Angle, 0.05, {'units': 'deg', 'default_units': 'arcmin'}, 0.05, 3),
        (Angle, 3, {'units': 'arcmin'}, 3, 0.05),
        (Angle, 1, {'units': 'arcsec'}, 1, 0.0002777777777777778),
        (Angle, -29.74516667, {}, -29.74516667, -29.74516667),
        (Angle, '-29.74516667', {}, -29.74516667, -29.74516667),
        (Angle, 'aaaaa', {}, ValueError, None),
        (Angle, -0.519151094946, {'units': 'rad'}, -0.519151094946, -29.745166670001282)
    ]:
        def constructor():
            return parameter_type(value=input_value,
                                  name="my-parameter-name",
                                  **format_args
                                  )

        if isinstance(outcome, type) and issubclass(outcome, Exception):
            with pytest.raises(outcome):
                constructor()
        else:
            # this also sets the default value
            parameter = constructor()

            assert parameter.value == outcome
            assert parameter.get_value_in_default_units() == outcome_default_format

            # setting value during request
            assert parameter.set_par(input_value) == outcome_default_format
            assert parameter.value == outcome
            if 'units' in format_args:
                assert parameter.get_value_in_units(format_args['units']) == outcome

@pytest.mark.fast
def test_time_parameter():
    for parameter_type, input_value, format_args, outcome, outcome_default_format in [
        (Time, '2017-03-06T13:26:48.000', {'T_format': 'isot'}, '2017-03-06T13:26:48.000', '2017-03-06T13:26:48.000'),
        (Time, 57818.560277777775, {'T_format': 'mjd'}, 57818.560277777775, '2017-03-06T13:26:48.000'),
        (Time, '57818.560277777775', {'T_format': 'mjd'}, 57818.560277777775, '2017-03-06T13:26:48.000'),
        (Time, '2017-03-06Z13:26:48.000', {'T_format': 'isot'}, ValueError, None),
        (Time, 'aaaa', {'T_format': 'mjd'}, ValueError, None),
        (TimeDelta, 1000., {'delta_T_format': 'sec'}, np.float64(1000.), np.float64(1000.)),
        (TimeDelta, '1000.', {'delta_T_format': 'sec'}, np.float64(1000.), np.float64(1000.)),
        (TimeDelta, 'aaaa', {'delta_T_format': 'sec'}, ValueError, None)
    ]:
        def constructor():
            return parameter_type(value=input_value,
                                  name="my-parameter-name",
                                  **format_args
                                  )

        if isinstance(outcome, type) and issubclass(outcome, Exception):
            with pytest.raises(outcome):
                constructor()
        else:
            # this also sets the default value
            parameter = constructor()

            assert parameter.value == outcome
            assert parameter.get_value_in_default_units() == outcome_default_format

            # setting value during request
            assert parameter.set_par(input_value) == outcome_default_format
            assert parameter.value == outcome
            if 'T_format' in format_args:
                assert parameter.get_value_in_units(format_args['T_format']) == outcome
            if 'delta_T_format' in format_args:
                assert parameter.get_value_in_units(format_args['delta_T_format']) == outcome

@pytest.mark.fast
def test_param_range():
    for parameter_type_p1, value_p1, parameter_type_p2, value_p2, outcome, outcome_message in [
        (Time, '2017-03-06T13:26:48.000', Time, '2017-03-06T13:26:49.000', None, None),
        (Float, '2017', Time, '2017-03-06T13:26:48.000', RuntimeError, 'pars must be of the same type'),
        (float, '2017', Time, '2017-03-06T13:26:48.000', RuntimeError, 'pars must be of the same type'),
        (float, '2017', float, '2018', RuntimeError, 'both p1 and p2 must be Parameters objects, found float for p1 and float for p2')
    ]:
        p1 = parameter_type_p1(value_p1)
        p2 = parameter_type_p2(value_p2)

        def constructor(par1, par2):
            return ParameterRange(par1, par2, 'test-range')
        if isinstance(outcome, type) and issubclass(outcome, Exception):
            with pytest.raises(outcome, match=outcome_message):
                constructor(p1, p2)
        else:
            p_range = constructor(p1, p2)
            assert p_range.to_list() == [p1, p2]

@pytest.mark.fast
def test_param_tuple():
    pf1 = Float('2017')
    pf2 = Float('2017')
    pf3 = Float('2017')
    pf4 = Float('2017')

    pi1 = Integer('2017')
    pi2 = Integer('2017')
    pi3 = Integer('2017')
    pi4 = Integer('2017')
    for parameter_list, outcome, outcome_message in [
        ([pf1, pf2, pf3, pf4], None, None),
        ([pf1, pi2, pi3, pi4], RuntimeError, 'pars must be of the same type'),
        ([pi1, pi2, pi3, 2017], RuntimeError, 'all the members of the tuple must be Parameters instances, found a int'),
    ]:

        def constructor():
            return ParameterTuple(parameter_list, 'test-tuple')

        if isinstance(outcome, type) and issubclass(outcome, Exception):
            with pytest.raises(outcome, match=outcome_message):
                constructor()
        else:
            p_range = constructor()
            assert all(map(lambda x, y: x == y, p_range.to_list(), parameter_list))

@pytest.mark.fast
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
            (Float, None, None),
            (Float, '', None),
            (Integer, 25, 25),
            (Integer, None, None),
            (Integer, '', None),
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
            assert parameter.get_value_in_default_units() == parameter.value

            assert parameter.value == outcome
            assert type(parameter.value) == type(outcome)

            # setting value during request
            
            assert parameter.set_par(input_value) == outcome
            assert parameter.value == outcome
            assert type(parameter.value) == type(outcome)
