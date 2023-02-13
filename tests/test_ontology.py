from cdci_data_analysis.analysis.ontology import Ontology

oda_prefix = 'http://odahub.io/ontology#'

def test_ontology_hierarchy():
    onto = Ontology('oda-ontology.owl')
    
    hierarchy_list = onto.get_parameter_hierarchy('oda:PointOfInterestRA')
    assert f'{oda_prefix}RightAscension' in hierarchy_list
    assert hierarchy_list.index(f'{oda_prefix}PointOfInterestRA') < \
           hierarchy_list.index(f'{oda_prefix}RightAscension') < \
           hierarchy_list.index(f'{oda_prefix}Angle') < \
           hierarchy_list.index(f'{oda_prefix}Float') 
           
    hierarchy_list = onto.get_parameter_hierarchy('oda:Energy_keV')
    assert f'{oda_prefix}Energy' in hierarchy_list
    assert hierarchy_list.index(f'{oda_prefix}Energy_keV') < hierarchy_list.index(f'{oda_prefix}Float')
    
    
def test_ontology_format():
    onto = Ontology('oda-ontology.owl')
    
    format_uri = onto.get_parameter_format('oda:StartTimeMJD', return_uri=True)
    assert format_uri == f'{oda_prefix}MJD'
    
    format = onto.get_parameter_format('oda:StartTimeISOT')
    assert format == 'isot'
    
    format = onto.get_parameter_format('oda:TimeInstant')
    assert format is None
    
    onto.parse_extra_ttl("""@prefix oda: <http://odahub.io/ontology#> . 
                            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
                            oda:foo a oda:TimeInstant ; 
                                    oda:format oda:MJD . """)
    format = onto.get_parameter_format('oda:foo')
    assert format == 'mjd'
    

def test_ontology_unit():
    onto = Ontology('oda-ontology.owl')
    
    unit_uri = onto.get_parameter_unit('oda:TimeDays', return_uri=True)
    assert unit_uri == f'{oda_prefix}Day'
    
    unit = onto.get_parameter_unit('oda:DeclinationDegrees')
    assert unit == 'deg'

    unit = onto.get_parameter_unit('oda:Energy')
    assert unit is None
    
    onto.parse_extra_ttl("""@prefix oda: <http://odahub.io/ontology#> . 
                        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
                        oda:foo a oda:TimeDelta, oda:par_second . """)
    unit = onto.get_parameter_unit('oda:foo')
    assert unit == 's'
    
    onto.parse_extra_ttl("""@prefix oda: <http://odahub.io/ontology#> . 
                        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
                        oda:bar a oda:TimeDelta ;
                                oda:unit oda:Hour . """)
    unit = onto.get_parameter_unit('oda:bar')
    assert unit == 'h'
