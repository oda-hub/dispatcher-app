import rdflib as rdf
from rdflib.namespace import RDFS, RDF, OWL

class Ontology:
    def __init__(self, ontology_path):
        #TODO: it's not optimal to read ontology on every init
        #      it should be cached globally 
        #      but with possibility to update without restarting dispatcher
        self.g = rdf.Graph()
        self.g.parse(ontology_path)
        ODA = rdf.Namespace("http://odahub.io/ontology#")
        self.g.bind('oda', ODA)
    
    def parse_extra_ttl(self, extra_ttl):
        self.g.parse(data = extra_ttl)
        
    def get_parameter_hierarchy(self, param_uri):
        if param_uri.startswith("http"): param_uri = f"<{param_uri}>"
        query = """
        select ?mid ( count(?mid2) as ?midcount ) where { 
        %s  (rdfs:subClassOf|a)* ?mid . 
        
        ?mid rdfs:subClassOf* ?mid2 .
        ?mid2 rdfs:subClassOf* oda:WorkflowParameter .
        }
        group by ?mid
        order by desc(?midcount)
        """ % ( param_uri )

        qres = self.g.query(query)
        
        return [str(row[0]) for row in qres]
        
    def get_parameter_format(self, param_uri, return_uri = False):
        if param_uri.startswith("http"): param_uri = f"<{param_uri}>"

        if return_uri:
            query = "SELECT ?format_uri WHERE { "
        else:
            query = """
            SELECT ?format WHERE { 
                ?format_uri oda:symbol ?format .
            """
        
        query += """
                {
                    %s (rdfs:subClassOf|a)* [
                    a owl:Restriction ;
                    owl:onProperty oda:format ;
                    owl:hasValue ?format_uri ;
                    ]
                }
                UNION
                {
                %s (rdfs:subClassOf|a)* [
                    oda:format ?format_uri ;
                    ]
                }
            }
            """ % (param_uri, param_uri)

        qres = self.g.query(query)
        
        if len(qres) > 1:
            raise RuntimeError('Ambiguous format for owl_uri ', param_uri) 
            # TODO: does it ever possible?
            #       probably RequestNotUnderstood is better for reporting
        
        if len(qres) == 0: return None
        
        return str(list(qres)[0][0])
        
    def get_parameter_unit(self, param_uri, return_uri = False):
        if param_uri.startswith("http"): param_uri = f"<{param_uri}>"

        if return_uri:
            query = "SELECT ?unit_uri WHERE {"
        else:
            query = """ 
            SELECT ?unit WHERE {
                ?unit_uri oda:symbol ?unit .
            """
        
        query += """
        {
        %s (rdfs:subClassOf|a)* [
            a owl:Restriction ;
            owl:onProperty oda:unit ;
            owl:hasValue ?unit_uri ;
            ]
        }
        UNION
        {
        %s (rdfs:subClassOf|a)* [
            oda:unit ?unit_uri ;
            ]
        }
        }
        """ % (param_uri, param_uri)
        
        qres = self.g.query(query)
        if len(qres) > 1:
            raise RuntimeError('Ambiguous unit for owl_uri ', param_uri) 
            # TODO: does it ever possible?
            #       probably RequestNotUnderstood is better for reporting
        
        if len(qres) == 0: return None
    
        return str(list(qres)[0][0])
        
    