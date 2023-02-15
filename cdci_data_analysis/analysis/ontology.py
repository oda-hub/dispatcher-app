import rdflib as rdf
import logging
from cdci_data_analysis.analysis.exceptions import RequestNotUnderstood

logger = logging.getLogger(__name__)

class Ontology:
    def __init__(self, ontology_path):
        #TODO: it's not optimal to read ontology on every init
        #      it should be cached globally 
        #      but with possibility to update without restarting dispatcher
        self.g = rdf.Graph()
        self.g.parse(ontology_path)
        ODA = rdf.Namespace("http://odahub.io/ontology#")
        self.g.bind('oda', ODA)
    
    def _get_symb(self, uri):
        s_qres = self.g.query( """SELECT ?symb WHERE { 
                                  { <%s> oda:symbol ?symb } 
                                    UNION
                                  { <%s> rdfs:label ?symb }
                                } """ % (uri, uri) 
                            )
        if len(s_qres) == 0: return uri.split('#')[1]
        return str(list(s_qres)[0][0])
    
    def parse_extra_ttl(self, extra_ttl):
        self.g.parse(data = extra_ttl)
        
    def get_parameter_hierarchy(self, param_uri):
        param_uri_m = f"<{param_uri}>" if param_uri.startswith("http") else param_uri
        query = """
        select ?mid ( count(?mid2) as ?midcount ) where { 
        %s  (rdfs:subClassOf|a)* ?mid . 
        
        ?mid rdfs:subClassOf* ?mid2 .
        ?mid2 rdfs:subClassOf* oda:WorkflowParameter .
        }
        group by ?mid
        order by desc(?midcount)
        """ % ( param_uri_m )

        qres = self.g.query(query)
        
        hierarchy = [str(row[0]) for row in qres]
        if len(hierarchy) > 0:
            return hierarchy  
        else:
            logger.warning("%s is not in ontology or not an oda:WorkflowParameter", param_uri)
            return [ param_uri ]
        
    def get_parameter_format(self, param_uri, return_uri = False):
        if param_uri.startswith("http"): param_uri = f"<{param_uri}>"
       
        query = """ SELECT ?format_uri WHERE { 
                {
                    %s (rdfs:subClassOf|a)* [
                    a owl:Restriction ;
                    owl:onProperty oda:format ;
                    owl:hasValue ?format_uri ;
                    ]
                }
                UNION
                {
                %s oda:format ?format_uri ;
                   
                }
            }
            """ % (param_uri, param_uri)

        qres = self.g.query(query)
        
        if len(qres) > 1:
            raise RequestNotUnderstood('Ambiguous format for owl_uri ', param_uri) 
        
        if len(qres) == 0: return None
        
        uri = str(list(qres)[0][0])
        if not return_uri:
            return self._get_symb(uri)
        return uri
        
    def get_parameter_unit(self, param_uri, return_uri = False):
        if param_uri.startswith("http"): param_uri = f"<{param_uri}>"

        query = """SELECT ?unit_uri WHERE {
        {
        %s (rdfs:subClassOf|a)* [
            a owl:Restriction ;
            owl:onProperty oda:unit ;
            owl:hasValue ?unit_uri ;
            ]
        }
        UNION
        {
        %s oda:unit ?unit_uri ; 
        }
        }
        """ % (param_uri, param_uri)
        
        qres = self.g.query(query)
        if len(qres) > 1:
            raise RequestNotUnderstood('Ambiguous unit for owl_uri ', param_uri) 
        
        if len(qres) == 0: return None
        
        uri = str(list(qres)[0][0])
        
        if not return_uri:
            return self._get_symb(uri)        
        return uri
        
    def get_limits(self, param_uri):
        if param_uri.startswith("http"): param_uri = f"<{param_uri}>"

        query = """SELECT ?limit WHERE {
        {
        %s (rdfs:subClassOf|a)* [
            a owl:Restriction ;
            owl:onProperty oda:%s_limit ;
            owl:hasValue ?limit ;
            ]
        }
        UNION
        {
        %s oda:%s_limit ?limit ;
        }
        }
        """
        
        qres_ll = self.g.query(query % (param_uri, 'lower', param_uri, 'lower'))
        qres_ul = self.g.query(query % (param_uri, 'upper', param_uri, 'upper'))
        
        if len(qres_ll) == 0: 
            ll = None
        elif len(qres_ll) == 1:
            ll = float(list(qres_ll)[0][0])
        else:
            ll = max([float(row[0]) for row in qres_ll])
            logger.warning('Ambiguous lower_limit, using the most restrictive %s', ll)
            
        if len(qres_ul) == 0: 
            ul = None
        elif len(qres_ul) == 1:
            ul = float(list(qres_ul)[0][0])
        else:
            ul = min([float(row[0]) for row in qres_ul])
            logger.warning('Ambiguous upper_limit, using the most restrictive %s', ul)
        
        return (ll, ul)
    
    def get_allowed_values(self, param_uri):
        if param_uri.startswith("http"): param_uri = f"<{param_uri}>"

        # either uri is for Individual with allowed_values directly set, then don't go to superclass restrictions 
        # or read all from superclass 
     
        
        
        return None
        return [] #it's not used anywhere
        return ['a', 'b']
    