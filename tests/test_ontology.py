from cdci_data_analysis.analysis.ontology import Ontology

oda_prefix = 'http://odahub.io/ontology#'

def test_ontology_hierarchy():
    onto = Ontology('oda-ontology.owl')
    
    hierarchy_list = onto.get_parameter_hierarchy('oda:PointOfInterestRA')
    expected_h = [oda_prefix + x for x in ('PointOfInterestRA',
                                           'RightAscension',
                                           'Angle',
                                           'Float',
                                           'NumericParameter',
                                           'WorkflowParameter')]
    assert hierarchy_list == expected_h
    
def test_ontology_format():
    onto = Ontology('oda-ontology.owl')
    
    format_dict = onto.get_parameter_format('oda:StartTimeISOT')
    assert format_dict == {'T_format': 'isot'}
    
    format_dict = onto.get_parameter_format('oda:TimeInstant')
    assert format_dict == None
    
def test_ontology_unit():
    onto = Ontology('oda-ontology.owl')
    
    unit = onto.get_parameter_unit('oda:DeclinationDegrees')
    assert unit == 'deg'

    unit = onto.get_parameter_unit('oda:Declination')
    assert unit == None
    
