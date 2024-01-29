import pytest
import rdflib as rdf
from rdflib.namespace import XSD
from rdflib.compare import isomorphic
from cdci_data_analysis.analysis.ontology import Ontology
from cdci_data_analysis.analysis.exceptions import RequestNotUnderstood

oda_prefix = 'http://odahub.io/ontology#'
xsd_prefix = 'http://www.w3.org/2001/XMLSchema#'
unit_prefix = 'http://odahub.io/ontology/unit#'
add_prefixes = """
            @prefix oda: <http://odahub.io/ontology#> . 
            @prefix unit: <http://odahub.io/ontology/unit#> . 
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            """
ontology_path = 'tests/oda-ontology.ttl'

@pytest.fixture
def onto(scope='module'):
    return Ontology(ontology_path)

@pytest.mark.parametrize('empty_str', ['', ' ', '\t', '\n'])
def test_ontology_empty_ttl(onto, empty_str):
    onto.parse_extra_triples(empty_str)

def test_ontology_hierarchy(onto):
    hierarchy_list = onto.get_parameter_hierarchy('oda:PointOfInterestRA')
    assert f'{oda_prefix}RightAscension' in hierarchy_list
    assert hierarchy_list.index(f'{oda_prefix}PointOfInterestRA') < \
           hierarchy_list.index(f'{oda_prefix}RightAscension') < \
           hierarchy_list.index(f'{oda_prefix}Angle') < \
           hierarchy_list.index(f'{oda_prefix}Float') 
           
    hierarchy_list = onto.get_parameter_hierarchy('oda:Energy_keV')
    assert f'{oda_prefix}Energy' in hierarchy_list
    assert hierarchy_list.index(f'{oda_prefix}Energy') < hierarchy_list.index(f'{oda_prefix}Float')

    hierarchy_list = onto.get_parameter_hierarchy('oda:StartTimeISOT')
    assert f'{oda_prefix}TimeInstant' in hierarchy_list
    assert hierarchy_list.index(f'{oda_prefix}StartTime') < hierarchy_list.index(f'{oda_prefix}TimeInstant')

    hierarchy_list = onto.get_parameter_hierarchy('oda:StartTime')
    assert f'{oda_prefix}TimeInstant' in hierarchy_list
    assert hierarchy_list.index(f'{oda_prefix}StartTime') < hierarchy_list.index(f'{oda_prefix}TimeInstant')


@pytest.mark.parametrize('owl_uri', ['http://www.w3.org/2001/XMLSchema#bool', 'http://odahub.io/ontology#Unknown'])
def test_ontology_unknown(onto, owl_uri, caplog):
    hierarchy_list = onto.get_parameter_hierarchy(owl_uri)
    assert hierarchy_list == [owl_uri]
    assert f"{owl_uri} is not in ontology or not an oda:WorkflowParameter" in caplog.text
    
    
@pytest.mark.parametrize("owl_uri,expected,extra_ttl,return_uri", 
                         [('oda:StartTimeMJD', f'{oda_prefix}MJD', None, True),
                          ('oda:StartTimeISOT', 'isot', None, False),
                          ('oda:TimeInstant', None, None, False),
                          ('http://odahub.io/ontology#Unknown', None, None, False),
                          ('oda:foo', 'mjd', """@prefix oda: <http://odahub.io/ontology#> . 
                                                @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
                                                oda:foo rdfs:subClassOf oda:TimeInstant ; 
                                                        oda:format oda:MJD . """, False)
                          ])
def test_ontology_format(onto, owl_uri, expected,extra_ttl, return_uri):
    if extra_ttl is not None:
        onto.parse_extra_triples(extra_ttl)
    format = onto.get_parameter_format(owl_uri, return_uri=return_uri)
    assert format == expected
    
@pytest.mark.parametrize("owl_uri, expected, extra_ttl, return_uri",
                         [('oda:TimeIntervalDays', f'{unit_prefix}Day', None, True),
                          ('oda:DeclinationDegrees', 'deg', None, False),
                          ('oda:Energy', None, None, False),
                          ('http://odahub.io/ontology#Unknown', None, None, False),
                          ('oda:spam', 's', """@prefix oda: <http://odahub.io/ontology#> . 
                                               @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
                                               oda:spam rdfs:subClassOf oda:TimeInterval, oda:second . """, False),
                          ('oda:eggs', 'hour', """@prefix oda: <http://odahub.io/ontology#> . 
                                               @prefix unit: <http://odahub.io/ontology/unit#> . 
                                               oda:eggs a oda:TimeInterval ;
                                                        oda:unit unit:Hour . """, False)
                         ])
def test_ontology_unit(onto, owl_uri, expected, extra_ttl, return_uri):
    if extra_ttl is not None:
        onto.parse_extra_triples(extra_ttl)
    unit = onto.get_parameter_unit(owl_uri, return_uri=return_uri)
    assert unit == expected
    
def test_ambiguous_unit(onto):
    onto.parse_extra_triples("""@prefix oda: <http://odahub.io/ontology#> .
                            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
                            @prefix unit: <http://odahub.io/ontology/unit#> . 
                            oda:Energy_EeV rdfs:subClassOf oda:Energy_TeV ;
                                           oda:unit unit:EeV .""")
    with pytest.raises(RequestNotUnderstood):
        onto.get_parameter_unit('oda:Energy_EeV')

@pytest.mark.parametrize("owl_uri, expected, extra_ttl",
                         [('oda:Float', (None, None), ""),
                          ('http://odahub.io/ontology#Unknown', (None, None), ""),
                          ('oda:Percentage', (0, 100), ""), # Class
                          ('oda:Float_w_lim', (0, 1), """@prefix oda: <http://odahub.io/ontology#> .
                                                         @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
                                                         oda:Float_w_lim rdfs:subClassOf oda:Float ;
                                                                    oda:lower_limit 0 ;
                                                                    oda:upper_limit 1 ."""),
                          ('oda:sec_quart', (25, 50), """@prefix oda: <http://odahub.io/ontology#> .
                                                         @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
                                                         oda:sec_quart rdfs:subClassOf oda:Percentage ;
                                                                        oda:lower_limit 25 ;
                                                                        oda:upper_limit 50 .""")
                         ])
def test_ontology_limits(onto, owl_uri, expected, extra_ttl):
    if extra_ttl is not None:
        onto.parse_extra_triples(extra_ttl)
    limits = onto.get_limits(owl_uri)
    assert limits == expected
    
@pytest.mark.parametrize(
    "owl_uri, expected, extra_ttl",
    [('oda:String', None, None),
    ('oda:PhotometricBand', ['b', 'g', 'H', 'i', 'J', 'K', 'L', 'M', 'N', 'Q', 'r', 'u', 'v', 'y', 'z'], None),
    ('oda:VisibleBand', ['b', 'g', 'r', 'v'], None),
    ('oda:custom', ['a', 'b'], """@prefix oda: <http://odahub.io/ontology#> .
                                @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
                                oda:custom rdfs:subClassOf oda:String ;
                                            oda:allowed_value "a" ;
                                            oda:allowed_value "b" ."""),
    ('oda:wrong_visible', ['b', 'g'], """@prefix oda: <http://odahub.io/ontology#> .
                                      @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
                                        oda:wrong_visible rdfs:subClassOf oda:VisibleBand ;
                                        oda:allowed_value "a" ;
                                        oda:allowed_value "b" ;
                                        oda:allowed_value "g" .""")
    ])
def test_ontology_allowed_values(onto, owl_uri, expected, extra_ttl):
    if extra_ttl is not None:
        onto.parse_extra_triples(extra_ttl)
    allowed_values = onto.get_allowed_values(owl_uri)
    if expected is None:
        assert allowed_values is None
    else:
        assert sorted(allowed_values) == sorted(expected)

@pytest.mark.parametrize("par_uri, datatype",
                         [('oda:Integer', XSD.integer),
                          ('oda:Float', XSD.float),
                          ('oda:Percentage', XSD.float),
                          ('oda:Energy_keV', XSD.float),
                          ('xsd:string', XSD.string),
                          ('oda:Unknown', None),
                          ])
def test_datatype_restriction(onto, par_uri, datatype):
    assert onto._get_datatype_restriction(par_uri) == datatype
        
     
def test_parsing_unit_annotation(onto):
    g, g_expect = rdf.Graph(), rdf.Graph()
    annotated_ttl = add_prefixes + """
        oda:someEnergy rdfs:subClassOf oda:Energy ;
                    oda:unit    unit:keV .
        """ 
    g.parse(data = annotated_ttl)
    
    expected = annotated_ttl + """
        oda:someEnergy rdfs:subClassOf [
                    a owl:Restriction ;
                    owl:onProperty oda:has_unit ;
                    owl:hasValue unit:keV 
                    ] .
        """
    g_expect.parse(data = expected)
    
    onto.parse_oda_annotations(g)
    
    assert isomorphic(g, g_expect)
    
    with pytest.raises(RuntimeError):
        annotated_ttl = add_prefixes + """
            oda:someEnergy rdfs:subClassOf oda:Energy ;
                        oda:unit    unit:keV ;
                        oda:unit    unit:MeV .
            """ 
        g.parse(data = annotated_ttl)
        onto.parse_oda_annotations(g)
    
def test_parsing_format_annotation(onto):
    g, g_expect = rdf.Graph(), rdf.Graph()
    annotated_ttl = add_prefixes + """
        oda:someTime rdfs:subClassOf oda:TimeInstant ;
                    oda:format    oda:ISOT .
        """ 
    g.parse(data = annotated_ttl)
    
    expected = annotated_ttl + """
        oda:someTime rdfs:subClassOf [
                    a owl:Restriction ;
                    owl:onProperty oda:has_format ;
                    owl:hasValue oda:ISOT 
                    ] .
        """
    g_expect.parse(data = expected)
    
    onto.parse_oda_annotations(g)
    
    assert isomorphic(g, g_expect)
    
    with pytest.raises(RuntimeError):
        annotated_ttl = add_prefixes + """
            oda:someTime rdfs:subClassOf oda:TimeInstant ;
                    oda:format    oda:ISOT ;
                    oda:format    oda:MJD . 
            """ 
        g.parse(data = annotated_ttl)
        onto.parse_oda_annotations(g)
    
def test_parsing_allowedval_annotation(onto):
    g, g_expect = rdf.Graph(), rdf.Graph()
    annotated_ttl = add_prefixes + """
        oda:someString rdfs:subClassOf oda:String ;
                    oda:allowed_value  "a", "b", "c" .
        """ 
    g.parse(data = annotated_ttl)
    
    expected = annotated_ttl + """
        oda:someString rdfs:subClassOf [
                    a owl:Restriction ;
                    owl:onProperty oda:value ;
                    owl:allValuesFrom [
                        a rdfs:Datatype ;
                        owl:oneOf ("a" "b" "c") ] 
                    ] .
        """
    g_expect.parse(data = expected)
    
    onto.parse_oda_annotations(g)
    
    assert isomorphic(g, g_expect)

restr_dt_tmpl = """
    %s rdfs:subClassOf [
    a owl:Restriction ;
    owl:onProperty oda:value ;
    owl:allValuesFrom [
        a rdfs:Datatype ;
        owl:onDatatype %s ;
        owl:withRestrictions ( %s )
        ] 
    ] . 
    """

@pytest.mark.parametrize("input_ttl, expected_restr",
                         [("""oda:someFloat rdfs:subClassOf oda:Float ;
                                            oda:lower_limit  0 .
                           """,
                           restr_dt_tmpl % ('oda:someFloat', 
                                            'xsd:float', 
                                            '[xsd:minInclusive "0.0"^^xsd:float ]')),
                          
                          ("""oda:someFloat rdfs:subClassOf oda:Float ;
                                            oda:upper_limit  5.0 .
                           """,
                           restr_dt_tmpl % ('oda:someFloat', 
                                            'xsd:float', 
                                            '[xsd:maxInclusive "5.0"^^xsd:float ]')),
                          
                          ("""oda:someFloat rdfs:subClassOf oda:Float ;
                                            oda:lower_limit  -1 ;
                                            oda:upper_limit  5.2 .
                           """,
                           restr_dt_tmpl % ('oda:someFloat', 
                                            'xsd:float', 
                                            """ [ xsd:minInclusive "-1.0"^^xsd:float ]
                                                [ xsd:maxInclusive "5.2"^^xsd:float ]
                                            """)),
                          
                          ("""oda:someInt rdfs:subClassOf oda:Integer ;
                                            oda:lower_limit  0 .
                           """,
                           restr_dt_tmpl % ('oda:someInt', 
                                            'xsd:integer', 
                                            '[xsd:minInclusive "0"^^xsd:integer ]')),
                                                    
                          ("""oda:someEnergy rdfs:subClassOf oda:Energy_keV ;
                                            oda:lower_limit  35 .
                           """,
                           restr_dt_tmpl % ('oda:someEnergy', 
                                            'xsd:float', 
                                            '[xsd:minInclusive "35.0"^^xsd:float ]')),
                         ]) 
def test_parsing_limits_annotation(onto, input_ttl, expected_restr):
    g, g_expect = rdf.Graph(), rdf.Graph()
    annotated_ttl = add_prefixes + input_ttl
    g.parse(data = annotated_ttl)
    
    expected = annotated_ttl + expected_restr
    g_expect.parse(data = expected)
    
    onto.parse_oda_annotations(g)
    
    assert isomorphic(g, g_expect)
    

def test_parsing_lower_limit_multiple_exception(onto):
    g = rdf.Graph()
    with pytest.raises(RuntimeError):
        annotated_ttl = add_prefixes + """
            oda:someFloat rdfs:subClassOf oda:Float ;
                    oda:lower_limit   1.0 ;
                    oda:lower_limit   1.1 . 
            """ 
        g.parse(data = annotated_ttl)
        onto.parse_oda_annotations(g)

def test_parsing_upper_limit_multiple_exception(onto):
    g = rdf.Graph()
    with pytest.raises(RuntimeError):
        annotated_ttl = add_prefixes + """
            oda:someFloat rdfs:subClassOf oda:Float ;
                    oda:upper_limit   1.0 ;
                    oda:upper_limit   1.1 . 
            """ 
        g.parse(data = annotated_ttl)
        onto.parse_oda_annotations(g)
        
def test_parsing_limits_bad_value(onto):
    g = rdf.Graph()
    with pytest.raises(RuntimeError):
        annotated_ttl = add_prefixes + """
            oda:someFloat rdfs:subClassOf oda:Float ;
                    oda:lower_limit   "a" ;
                    oda:lower_limit   "b" . 
            """ 
        g.parse(data = annotated_ttl)
        onto.parse_oda_annotations(g)
        
def test_parsing_limits_bad_class(onto):
    g = rdf.Graph()
    with pytest.raises(RuntimeError):
        annotated_ttl = add_prefixes + """
            oda:someFloat rdfs:subClassOf oda:String ;
                    oda:lower_limit   0 ;
                    oda:lower_limit   1 . 
            """ 
        g.parse(data = annotated_ttl)
        onto.parse_oda_annotations(g)
