import pytest

from cdci_data_analysis.analysis.instrument import Instrument
from cdci_data_analysis.analysis.queries import (
    ProductQuery,
    SourceQuery,
    InstrumentQuery,
)
from cdci_data_analysis.analysis.parameters import (
    Parameter,
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
    InputProdList,
    DetectionThreshold,
    String,
    Boolean
)
from cdci_data_analysis.analysis.exceptions import RequestNotUnderstood

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
        (SpectralBoundary, 'ssss', RequestNotUnderstood, None, None),
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
    for parameter_type, input_value, format_args, outcome, outcome_default_units in [
        (Angle, -29.74516667, {'units': 'deg'}, -29.74516667, -29.74516667),
        (Angle, -29.74516667, {'units': 'deg', 'default_units': 'deg'}, -29.74516667, -29.74516667),
        (Angle, 3, {'units': 'arcmin', 'default_units': 'deg'}, 3, 0.05),
        (Angle, 3, {'units': 'arcmin', 'default_units': 'arcmin'}, 3, 3),
        (Angle, 0.05, {'units': 'deg', 'default_units': 'arcmin'}, 0.05, 3),
        (Angle, 3, {'units': 'arcmin'}, 3, 0.05),
        (Angle, 1, {'units': 'arcsec'}, 1, 0.0002777777777777778),
        (Angle, -29.74516667, {}, -29.74516667, -29.74516667),
        (Angle, '-29.74516667', {}, -29.74516667, -29.74516667),
        (Angle, 'aaaaa', {}, RequestNotUnderstood, None),
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
            assert parameter.get_value_in_default_units() == outcome_default_units
            # # backward compatibility
            # assert parameter.get_value_in_default_format() == outcome_default_units
            # setting value during request
            assert parameter.set_par(input_value) == outcome_default_units
            assert parameter.value == outcome
            if 'units' in format_args:
                assert parameter.get_value_in_units(format_args['units']) == outcome

@pytest.mark.fast
def test_time_parameter():
    for parameter_type, input_value, format_args, outcome, outcome_default_format in [
        (Time, '2017-03-06T13:26:48.000', {'T_format': 'isot'}, '2017-03-06T13:26:48.000', '2017-03-06T13:26:48.000'),
        (Time, 57818.560277777775, {'T_format': 'mjd'}, 57818.560277777775, '2017-03-06T13:26:48.000'),
        (Time, '57818.560277777775', {'T_format': 'mjd'}, 57818.560277777775, '2017-03-06T13:26:48.000'),
        (Time, '2017-03-06Z13:26:48.000', {'T_format': 'isot'}, RequestNotUnderstood, None),
        (Time, 'aaaa', {'T_format': 'mjd'}, RequestNotUnderstood, None),
        (TimeDelta, 1000., {'delta_T_format': 'sec'}, np.float64(1000.), np.float64(1000.)),
        (TimeDelta, '1000.', {'delta_T_format': 'sec'}, np.float64(1000.), np.float64(1000.)),
        (TimeDelta, 'aaaa', {'delta_T_format': 'sec'}, RequestNotUnderstood, None)
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
            # assert parameter.get_value_in_default_units() == outcome_default_format
            # backward compatibility
            assert parameter.get_value_in_default_format() == outcome_default_format
            # setting value during request
            assert parameter.set_par(input_value) == outcome_default_format
            assert parameter.value == outcome
            if 'T_format' in format_args:
                assert parameter.get_value_in_format(format_args['T_format']) == outcome
            if 'delta_T_format' in format_args:
                assert parameter.get_value_in_format(format_args['delta_T_format']) == outcome

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
            (Float, "aaaa", RequestNotUnderstood),
            (Float, None, None),
            (Float, '', None),
            (Integer, 25, 25),
            (Integer, None, None),
            (Integer, '', None),
            (Integer, 25., RequestNotUnderstood),
            (Integer, 25.64547871216879451687311, RequestNotUnderstood),
            (Integer, "25", 25),
            (Integer, "25.", RequestNotUnderstood),
            (Integer, "25.64547871216879451687311", RequestNotUnderstood),
            (Integer, "aaaa", RequestNotUnderstood)
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
            # backward compatibility
            assert parameter.get_value_in_default_format() == parameter.value
            assert parameter.value == outcome
            assert type(parameter.value) == type(outcome)

            # setting value during request
            
            assert parameter.set_par(input_value) == outcome
            assert parameter.value == outcome
            assert type(parameter.value) == type(outcome)


@pytest.mark.fast
def test_parameter_normalization_with_units():
    for parameter_type, input_value, outcome, args in [
        (DetectionThreshold, 25, 25.0, {'units': 'sigma'},),
        (DetectionThreshold, 25, RuntimeError, {'units': 'fake'},),
    ]:

        def constructor():
            return parameter_type(value=input_value, name="my-parameter-name", **args)

        if isinstance(outcome, type) and issubclass(outcome, Exception):
            with pytest.raises(outcome):
                constructor()
        else:
            # this also sets the default value
            parameter = constructor()

            # this is redundant
            assert parameter.get_value_in_default_units() == parameter.value
            # backward compatibility
            assert parameter.get_value_in_default_format() == parameter.value
            assert parameter.value == outcome
            assert type(parameter.value) == type(outcome)

            # setting value during request

            assert parameter.set_par(input_value) == outcome
            assert parameter.value == outcome
            assert type(parameter.value) == type(outcome)
            
@pytest.mark.fast
@pytest.mark.parametrize('uri, value, param_type', 
                         [('http://odahub.io/ontology#PointOfInterestRA', 0.0, Angle),
                          ('http://odahub.io/ontology#PointOfInterestDEC', 0.0, Angle),
                          ('http://odahub.io/ontology#StartTime', '2017-03-06T13:26:48.0', Time),
                          ('http://odahub.io/ontology#EndTime', '2017-03-06T13:26:48.0', Time),
                          ('http://odahub.io/ontology#TimeInstant', '2017-03-06T13:26:48.0', Time),
                          ('http://odahub.io/ontology#AstrophysicalObject', 'Mrk421', Name)])
def test_parameter_from_owl_uri(uri, value, param_type):
    param = Parameter.from_owl_uri(uri, value=value, name='example')
    assert isinstance(param, param_type)


def test_parameter_from_owl_uri_extra_param(caplog):
    Parameter.from_owl_uri('http://odahub.io/ontology#StartTime',
                           value='59830',
                           T_format='mjd',
                           units='d', # wrong parameter
                           name='example')
    assert "parameter units with value d not used to construct <class 'cdci_data_analysis.analysis.parameters.Time'>" in caplog.text

@pytest.mark.fast
def test_parameter_bounds():
    int_param = Integer(5, name = 'INT', min_value = 2, max_value = 8)
    fl_param = Float(5., name = 'FL', min_value = 2.2, max_value = 7.7)
    with pytest.raises(RequestNotUnderstood):
        int_param.value = 1
    with pytest.raises(RequestNotUnderstood):
        int_param.value = 10
    with pytest.raises(RequestNotUnderstood):
        fl_param.value = 1.2
    with pytest.raises(RequestNotUnderstood):
        fl_param.value = 8.3
    with pytest.raises(RequestNotUnderstood):
        Integer(1, name = 'INT', min_value = 2, max_value = 8)
    with pytest.raises(RequestNotUnderstood):
        Float(1.1, name = 'FL', min_value = 2.2, max_value = 7.7)
    with pytest.raises(RequestNotUnderstood):
        Integer(10, name = 'INT', min_value = 2, max_value = 8)
    with pytest.raises(RequestNotUnderstood):
        Float(8.2, name = 'FL', min_value = 2.2, max_value = 7.7)
    with pytest.raises(NotImplementedError):
        Parameter(value = 1, name = 'foo', min_value = 0, max_value=10)
        
@pytest.mark.fast 
def test_parameter_meta_data():
    bounded_parameter = Float(value = 1., name='bounded', min_value=0.1, max_value=2)
    choice_parameter = String(value = 'spam', name='choice', allowed_values=['spam', 'eggs', 'hams'])
    bool_parameter = Boolean(value = True, name = 'bool')
    assert bounded_parameter.reprJSONifiable() == [{'name': 'bounded', 
                                                    'units': None, 'value': 1.0, 
                                                    'restrictions': {'min_value': 0.1, 'max_value': 2.0}}]
    assert choice_parameter.reprJSONifiable() == [{'name': 'choice', 
                                                   'units': 'str', 
                                                   'value': 'spam',
                                                   'restrictions': {'allowed_values': ['spam', 'eggs', 'hams']}}]
    assert bool_parameter.reprJSONifiable() == [{'name': 'bool', 
                                                'units': None, 
                                                'value': True, 
                                                'restrictions': {'allowed_values': ['True', 'true', 'yes', '1', True, 
                                                                                    'False', 'false', 'no', '0', False]}}]
    
@pytest.mark.fast
@pytest.mark.parametrize('inval, iswrong, expected',
                         [('True', False, True),
                          ('true', False, True),
                          ('yes', False, True),
                          ('1', False, True),
                          (True, False, True),
                          
                          ('False', False, False),
                          ('false', False, False),
                          ('no', False, False),
                          ('0', False, False),
                          (False, False, False),
                          
                          ('Spam', True, False),
                          (5, True, False)])
def test_boolean_parameter(inval, iswrong, expected):
    if not iswrong:
        p = Boolean(inval)
        assert p.value == expected
    else:
        with pytest.raises(RequestNotUnderstood):
            Boolean(inval)