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
    TimeInterval,
    ParameterRange,
    ParameterTuple,
    Angle,
    InputProdList,
    DetectionThreshold,
    String,
    Boolean,
    StructuredParameter,
    PhosphorosFiltersTable
)
from cdci_data_analysis.analysis.exceptions import RequestNotUnderstood

import numpy as np


@pytest.mark.fast
@pytest.mark.parametrize("same_query", [True, False])
def test_repeating_parameters(caplog, same_query):
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

    if same_query:
        parameters_list = [p1, p2]
        with pytest.raises(RuntimeError):
            product_query = ProductQuery("test_product_query", parameters_list=parameters_list)
    else:
        product_query1 = ProductQuery("test_product_query1", parameters_list=[p1])
        product_query2 = ProductQuery("test_product_query2", parameters_list=[p2])
        
        query_dictionary = {"prod1": "test_product_query1",
                            "prod2": "test_product_query2",}

        instrument = Instrument(
            "empty-async",
            src_query=src_query,
            instrumet_query=instr_query,
            product_queries_list=[product_query1, product_query2],
            query_dictionary=query_dictionary,
            data_server_query_class=None,
        )
        
        assert instrument.get_par_by_name("duplicate-name", prod_name='prod1') == p1
        assert instrument.get_par_by_name("duplicate-name", prod_name='prod2') == p2
        
        assert instrument.get_par_by_name("duplicate-name") == p2
        assert 'Same parameter name' in caplog.text

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
    for parameter_type, input_value, outcome, outcome_in_default, e_units, expected_type in [
        (Energy, 10., 10., 10., 'keV', float),
        (SpectralBoundary, 10., 10., 10., 'keV', float),
        (SpectralBoundary, 10, 10., 10., 'keV', float),
        (SpectralBoundary, 10., 10., 0.01, 'eV', float),
        (SpectralBoundary, 10., RuntimeError, ..., 'W', None),
        (SpectralBoundary, 'ssss', RequestNotUnderstood, ...,  None, None),
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

            assert p_spectral_boundary.get_value_in_default_units() == outcome_in_default
            assert p_spectral_boundary.value == outcome
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
                                                    'restrictions': {'min_value': 0.1, 'max_value': 2.0},
                                                    'owl_uri': ["http://www.w3.org/2001/XMLSchema#float", "http://odahub.io/ontology#Float"]}]
    assert choice_parameter.reprJSONifiable() == [{'name': 'choice', 
                                                   'units': 'str', 
                                                   'value': 'spam',
                                                   'restrictions': {'allowed_values': ['spam', 'eggs', 'hams']},
                                                   'owl_uri': ["http://www.w3.org/2001/XMLSchema#str", "http://odahub.io/ontology#String"]}]
    assert bool_parameter.reprJSONifiable() == [{'name': 'bool', 
                                                'units': None, 
                                                'value': True, 
                                                'restrictions': {'allowed_values': ['True', 'true', 'yes', '1', True, 
                                                                                    'False', 'false', 'no', '0', False]},
                                                'owl_uri': ["http://www.w3.org/2001/XMLSchema#bool","http://odahub.io/ontology#Boolean"]}]
    
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
            
@pytest.mark.fast
@pytest.mark.parametrize(
    'uri, extra_ttl, use_ontology, value, param_type', 
    [('http://odahub.io/ontology#TimeInstant', None, False, '2017-03-06T13:26:48.000', Time),
    ('http://odahub.io/ontology#AstrophysicalObject', None, False, 'Mrk421', Name),
    ('http://odahub.io/ontology#Unknown', None, False, 'foo', Parameter),
    ('http://odahub.io/ontology#PointOfInterestRA', None, False, 0.0, Parameter),
    ('http://odahub.io/ontology#Unknown', None, True, 'foo', Parameter),
    ('http://odahub.io/ontology#PointOfInterestRA', None, True, 0.0, Angle),
    ('http://odahub.io/ontology#PointOfInterestDEC', None, True, 0.0, Angle),
    ('http://odahub.io/ontology#StartTime', None, True, '2017-03-06T13:26:48.000', Time),
    ('http://odahub.io/ontology#EndTimeISOT', None, True, '2017-03-06T13:26:48.000', Time),
    ('http://odahub.io/ontology#myminEnergy', 
    """@prefix oda: <http://odahub.io/ontology#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        oda:myminEnergy rdfs:subClassOf oda:Energy_keV .""", 
    True, 100, Energy),
    ])
def test_parameter_class_from_owl_uri(uri, extra_ttl, use_ontology, value, param_type):
    ontology_path = 'tests/oda-ontology.ttl' if use_ontology else None
    param = Parameter.from_owl_uri(uri, 
                                   value=value,
                                   extra_ttl = extra_ttl, 
                                   name='example', 
                                   ontology_path = ontology_path)
    assert param.__class__.__name__ == param_type.__name__
    assert param.value == value

def test_parameter_from_owl_uri_extra_param(caplog):
    Parameter.from_owl_uri('http://odahub.io/ontology#TimeInstant',
                           value='59830',
                           T_format='mjd',
                           units='d', # wrong parameter
                           name='example')
    assert ("parameter units with value d not used to construct " 
            "<class 'cdci_data_analysis.analysis.parameters.Time'>") in caplog.text

@pytest.mark.fast
@pytest.mark.parametrize(
    "uri, extra_ttl, value, format_override, expected_format",
    [('http://odahub.io/ontology#String', None, 'foo', None, None),
     ('http://odahub.io/ontology#StartTimeMJD', None, 57818.560277777775, None, 'mjd'),
     ('http://odahub.io/ontology#StartTimeMJD', None, '2017-03-06T13:26:48.000', 'isot', 'isot'),
     ('http://odahub.io/ontology#mystarttime', 
       """@prefix oda: <http://odahub.io/ontology#> . 
          @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
          oda:mystarttime rdfs:subClassOf oda:StartTime ; 
               oda:format oda:MJD . """, 
       57818.560277777775, None, 'mjd'),
    ])
def test_parameter_format_from_owl_uri(uri, extra_ttl, value, format_override, expected_format, caplog):
    kwargs = {'T_format': format_override} if format_override is not None else {}
    param = Parameter.from_owl_uri(uri, 
                                   value=value,
                                   extra_ttl = extra_ttl, 
                                   name='example', 
                                   ontology_path = 'tests/oda-ontology.ttl',
                                   **kwargs)
    if format_override:
        assert ("overriding ontology-derived value of the T_format keyword of %s "
                "with explicitly set value %s") % (Time, format_override) in caplog.text

    assert param.par_format == expected_format 

@pytest.mark.fast
@pytest.mark.parametrize(
    "uri, extra_ttl, unit_kw, unit_override, expected_unit",
    [('http://odahub.io/ontology#Float', None, 'units', None, None),
     ('http://odahub.io/ontology#Energy_keV', None, 'E_units', None, 'keV'),
     ('http://odahub.io/ontology#Energy_keV', None, 'E_units', 'MeV', 'MeV'),
     ('http://odahub.io/ontology#myEnergy', 
       """@prefix oda: <http://odahub.io/ontology#> .
          @prefix unit: <http://odahub.io/ontology/unit#> . 
          @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
          oda:myEnergy rdfs:subClassOf oda:Energy ; 
               oda:unit unit:keV . """, 
       'E_units', None, 'keV'),
     ('http://odahub.io/ontology#myEnergy', 
       """@prefix oda: <http://odahub.io/ontology#> .
          @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
          oda:myEnergy rdfs:subClassOf oda:Energy_MeV .""", 
       'E_units', None, 'MeV'),
    ])
def test_parameter_unit_from_owl_uri(uri, extra_ttl, unit_kw, unit_override, expected_unit, caplog):
    kwargs = {unit_kw: unit_override} if unit_override is not None else {}
    param = Parameter.from_owl_uri(uri, 
                                   value=1,
                                   extra_ttl = extra_ttl, 
                                   name='example', 
                                   ontology_path = 'tests/oda-ontology.ttl',
                                   **kwargs)
    if unit_override:
        assert ("overriding ontology-derived value of the %s keyword of %s "
                "with explicitly set value %s") % (unit_kw, param.__class__, unit_override) in caplog.text

    assert param.units == expected_unit 

@pytest.mark.fast    
@pytest.mark.parametrize(
    "uri, extra_ttl, min_override, max_override, expected_min, expected_max",
    [('http://odahub.io/ontology#Float', None, None, None, None, None),
     ('http://odahub.io/ontology#Percentage', None, None, None, 0, 100),
     ('http://odahub.io/ontology#Percentage', None, 25, 75, 25, 75),
     ('http://odahub.io/ontology#boundedFloat', 
       """@prefix oda: <http://odahub.io/ontology#> .
          @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
          oda:boundedFloat rdfs:subClassOf oda:Float ; 
               oda:lower_limit 1 ;
               oda:upper_limit 1000 . """, 
       None, None, 1, 1000),
     ('http://odahub.io/ontology#bfsc', 
       """@prefix oda: <http://odahub.io/ontology#> .
          @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
          oda:bfsc rdfs:subClassOf oda:Percentage .""", 
       None, None, 0, 100),
    ])
def test_parameter_bounds_from_owl_uri(uri, extra_ttl, min_override, max_override, expected_min, expected_max, caplog):
    kwargs = {}
    if min_override is not None:
        kwargs['min_value'] = min_override
    if max_override is not None:
        kwargs['max_value'] = max_override
    
    param = Parameter.from_owl_uri(uri, 
                                   value=50,
                                   extra_ttl = extra_ttl, 
                                   name='example', 
                                   ontology_path = 'tests/oda-ontology.ttl',
                                   **kwargs)
    if min_override is not None:
        assert ("overriding ontology-derived value of the min_value keyword of %s "
                "with explicitly set value %s") % (param.__class__, min_override) in caplog.text
    if max_override is not None:
        assert ("overriding ontology-derived value of the max_value keyword of %s "
                "with explicitly set value %s") % (param.__class__, max_override) in caplog.text

    assert param._min_value == expected_min
    assert param._max_value == expected_max

@pytest.mark.fast    
@pytest.mark.parametrize(
    "uri, extra_ttl, allowed_val_override, expected_allowed_val",
    [('http://odahub.io/ontology#String', None, None, None),
     ('http://odahub.io/ontology#VisibleBand', None, None, ["b", "g", "r", "v"]),
     ('http://odahub.io/ontology#VisibleBand', None, ["b", "g"], ["b", "g"]),
     ('http://odahub.io/ontology#photoband', 
       """@prefix oda: <http://odahub.io/ontology#> .
          @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
          oda:photoband rdfs:subClassOf oda:PhotometricBand ; 
               oda:allowed_value "b", "g" . """, 
       None, ["b", "g"]),
     ('http://odahub.io/ontology#visible', 
       """@prefix oda: <http://odahub.io/ontology#> .
          @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
          oda:visible rdfs:subClassOf oda:VisibleBand .""", 
       None, ["b", "g", "r", "v"]),
    ])
def test_parameter_allowedval_from_owl_uri(uri, extra_ttl, allowed_val_override, expected_allowed_val, caplog):
    kwargs = {'allowed_values': allowed_val_override} if allowed_val_override is not None else {}
    
    param = Parameter.from_owl_uri(uri, 
                                   value="b",
                                   extra_ttl = extra_ttl, 
                                   name='example', 
                                   ontology_path = 'tests/oda-ontology.ttl',
                                   **kwargs)
    if allowed_val_override is not None:
        assert ("overriding ontology-derived value of the allowed_values keyword of %s "
                "with explicitly set value %s") % (param.__class__, allowed_val_override) in caplog.text
    if expected_allowed_val is not None:
        assert sorted(param._allowed_values) == sorted(expected_allowed_val)
    else:
        assert param._allowed_values is None

@pytest.mark.fast
@pytest.mark.parametrize(
    "value, unit, default_unit, expected_value, expected_in_default_units",
    [(1, 'minute', 's', 1, 60),
     ('1', 'hour', 's', 1, 3600),
     ('1.', 'minute', 's', 1, 60),
     ('1.', None, 's', 1, 1),
     ]
)
def test_time_interval_param(value, unit, default_unit, expected_value, expected_in_default_units):
    ti = TimeInterval(value=value, units=unit, name='example', default_units=default_unit)
    assert ti.value == 1
    assert ti.get_value_in_default_units() == expected_in_default_units
    
@pytest.mark.fast
@pytest.mark.parametrize("value, unit", 
                         [('aaa', 's'),
                          (1, 'minuit')])
def test_time_interval_wrong_val(value, unit):
    with pytest.raises((RequestNotUnderstood, RuntimeError)):
        TimeInterval(value = value, units = unit)
        
def test_time_interval_bounds():
    ti = TimeInterval(value=3, units='s', min_value = 1, max_value=5)
    assert ti.value == 3 
    with pytest.raises(RequestNotUnderstood):
        TimeInterval(value=10, units='s', min_value = 1, max_value=5)
    
@pytest.mark.fast
def test_valid_phosphoros_table():
    tab_value = {'filter': ["Euclid|VIS.vis", "Euclid|NISP.Y"],
                 'flux': ["FLUX_DETECTION_TOTAL", "FLUX_Y_TOTAL"],
                 'flux_error': ["FLUXERR_DETECTION_TOTAL", "FLUXERR_Y_TOTAL"]}
    
    pht = PhosphorosFiltersTable(tab_value)
    
    assert pht.value == tab_value
    
@pytest.mark.fast
@pytest.mark.parametrize("tab_value", [
    # missing value
    {'filter': ["Euclid|VIS.vis", "Euclid|NISP.Y"],
     'flux': ["FLUX_DETECTION_TOTAL", "FLUX_Y_TOTAL"],
     'flux_error': ["FLUXERR_DETECTION_TOTAL"]},

    # empty value
    {'filter': ["Euclid|VIS.vis", "Euclid|NISP.Y"],
     'flux': ["FLUX_DETECTION_TOTAL", "FLUX_Y_TOTAL"],
     'flux_error': ["FLUXERR_DETECTION_TOTAL", ""]},
    
    # None value
    {'filter': ["Euclid|VIS.vis", "Euclid|NISP.Y"],
     'flux': ["FLUX_DETECTION_TOTAL", None],
     'flux_error': ["FLUXERR_DETECTION_TOTAL", "FLUXERR_Y_TOTAL"]},
    
    # Wrong format
    {'filter': ["Euclid|VIS.vis", "Euclid|NISP.Y"],
     'flux': [463, "FLUX_Y_TOTAL"],
     'flux_error': ["FLUXERR_DETECTION_TOTAL", "FLUXERR_Y_TOTAL"]},
    
    # unknown filter name
    {'filter': ["Euclid|VIS.vis", "UNKNOWN"],
     'flux': ["FLUX_DETECTION_TOTAL", "FLUX_Y_TOTAL"],
     'flux_error': ["FLUXERR_DETECTION_TOTAL", "FLUXERR_Y_TOTAL"]},
    
    # missing column
    {'filter': ["Euclid|VIS.vis", "Euclid|NISP.Y"],
     'flux': ["FLUX_DETECTION_TOTAL", "FLUX_Y_TOTAL"],
     },
    
    # Empty table
    {'filter': [],
     'flux': [],
     'flux_error': []},  
    
    ])
def test_invalid_phosphoros_table(tab_value):
    with pytest.raises(RequestNotUnderstood):
        PhosphorosFiltersTable(tab_value)
    

def test_structured_get_default_value():
    value = {"c": [15, 5], "a": [1, 2], "b": ["foo", "bar"]}
    expected = '{"a": [1, 2], "b": ["foo", "bar"], "c": [15, 5]}'
    
    stp = StructuredParameter(value)
    
    assert stp.get_default_value() == expected
    