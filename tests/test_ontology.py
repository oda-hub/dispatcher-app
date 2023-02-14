import pytest
from cdci_data_analysis.analysis.ontology import Ontology

oda_prefix = 'http://odahub.io/ontology#'
ontology_path = 'oda-ontology.owl'

@pytest.fixture
def onto(scope='module'):
    return Ontology(ontology_path)

def test_ontology_hierarchy(onto):
    hierarchy_list = onto.get_parameter_hierarchy('oda:PointOfInterestRA')
    assert f'{oda_prefix}RightAscension' in hierarchy_list
    assert hierarchy_list.index(f'{oda_prefix}PointOfInterestRA') < \
           hierarchy_list.index(f'{oda_prefix}RightAscension') < \
           hierarchy_list.index(f'{oda_prefix}Angle') < \
           hierarchy_list.index(f'{oda_prefix}Float') 
           
    hierarchy_list = onto.get_parameter_hierarchy('oda:Energy_keV')
    assert f'{oda_prefix}Energy' in hierarchy_list
    assert hierarchy_list.index(f'{oda_prefix}Energy_keV') < hierarchy_list.index(f'{oda_prefix}Float')


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
                                                oda:foo a oda:TimeInstant ; 
                                                        oda:format oda:MJD . """, False)
                          ])
def test_ontology_format(onto, owl_uri, expected,extra_ttl, return_uri):
    if extra_ttl is not None:
        onto.parse_extra_ttl(extra_ttl)
    format = onto.get_parameter_format(owl_uri, return_uri=return_uri)
    assert format == expected
    
@pytest.mark.parametrize("owl_uri, expected, extra_ttl, return_uri",
                         [('oda:TimeDays', f'{oda_prefix}Day', None, True),
                          ('oda:DeclinationDegrees', 'deg', None, False),
                          ('oda:Energy', None, None, False),
                          ('http://odahub.io/ontology#Unknown', None, None, False),
                          ('oda:spam', 's', """@prefix oda: <http://odahub.io/ontology#> . 
                                               oda:spam a oda:TimeDelta, oda:par_second . """, False),
                          ('oda:eggs', 'h', """@prefix oda: <http://odahub.io/ontology#> . 
                                               oda:eggs a oda:TimeDelta ;
                                                        oda:unit oda:Hour . """, False)
                         ])
def test_ontology_unit(onto, owl_uri, expected, extra_ttl, return_uri):
    if extra_ttl is not None:
        onto.parse_extra_ttl(extra_ttl)
    unit = onto.get_parameter_unit(owl_uri, return_uri=return_uri)
    assert unit == expected
    
def test_ambiguous_unit(onto):
    onto.parse_extra_ttl("""@prefix oda: <http://odahub.io/ontology#> .
                            @prefix rdfs: <rdfs	http://www.w3.org/2000/01/rdf-schema#> .
                            oda:Energy_EeV a oda:Energy_TeV ;
                                           oda:unit oda:EeV .""")
    with pytest.raises(RuntimeError):
        unit = onto.get_parameter_unit('oda:Energy_EeV')

    
@pytest.mark.parametrize("owl_uri, expected, extra_ttl",
                         [('oda:Float', (None, None), ""),
                          ('http://odahub.io/ontology#Unknown', (None, None), ""),
                          ('oda:ISGRIEnergy', (15, 800), ""), # Individual 
                          ('oda:Percentage', (0, 100), ""), # Class
                          ('oda:Float_w_lim', (0, 1), """@prefix oda: <http://odahub.io/ontology#> .
                                                         oda:Float_w_lim a oda:Float ;
                                                                    oda:lower_limit 0 ;
                                                                    oda:upper_limit 1 ."""),
                         ])
def test_ontology_limits(onto, owl_uri, expected, extra_ttl):
    if extra_ttl is not None:
        onto.parse_extra_ttl(extra_ttl)
    limits = onto.get_limits(owl_uri)
    assert limits == expected
    
def test_ontology_redefined_limits(onto, caplog):
    onto.parse_extra_ttl("""@prefix oda: <http://odahub.io/ontology#> .
                            oda:second_quartile a oda:Percentage ;
                                                oda:lower_limit 25 ;
                                                oda:upper_limit 50 .""")
    # strictly speaking, this is inconsistent definition, but let's allow it
    limits = onto.get_limits('oda:second_quartile')
    assert limits == (25, 50)
    assert 'Ambiguous lower_limit, using the most restrictive' in caplog.text
    assert 'Ambiguous upper_limit, using the most restrictive' in caplog.text