

from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)
import collections

__author__ = "Andrea Tramacere"


# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f

import json

from .parameters import *





@decorator.decorator
def check_is_base_query(func,prod_list,*args, **kwargs):
    _check_is_base_query(prod_list)

    return func(prod_list, *args, **kwargs)


def _check_is_base_query(_list):
    for _item in _list:
        if isinstance(_item,BaseQuery):
            pass
        else:
            raise RuntimeError('each member has to be a BaseQuery instance')


class BaseQuery(object):


    def __init__(self,name,_list):


        if _list is None:
            _list=[]

        self.name=name
        self._parameters_structure=_list

        self._parameters_list=self._build_parameters_list(_list)
        self._build_par_dictionary()


        self.product=None



    @property
    def parameters(self):
        return self._parameters_list

    @property
    def par_names(self):
        return [p1.name for p1 in self._parameters_list ]

    def get_par_by_name(self,name):
        p=None
        for p1 in self._parameters_list:
            if p1.name==name:
                p=p1
        if p is None:
            raise  Warning('parameter',name,'not found')
        return p

    def set_par_value(self,name,value):
        p=self.get_par_by_name(name)
        print('get par',p.name,'set value',value)
        if p is not None:
            p.value=value

    def _build_parameters_list(self,_list):

        _l = []
        if _list is None:
            pass
        else:

            for p in _list:
                if isinstance(p, Parameter):
                    _l.append(p)
                else:
                    _l.extend(p.to_list())
        return _l

    def show_parameters_list(self):

        print ("-------------")
        for par in self._parameters_list:
            self._show_parameter(par,indent=2)
        print("-------------")



    def show_parameters_structure(self):

        print ("-------------")
        for par in self._parameters_structure:

            if type(par)==ParameterGroup:

                self._show_parameter_group(par,indent=2)

            if type(par) == ParameterRange:

                self._show_parameter_range(par,indent=2)

            if isinstance(par,Parameter):

                self._show_parameter(par,indent=2)

            if isinstance(par,ParameterTuple):
                self._show_parameter_tuple(par, indent=2)

        print("-------------")

    def _show_parameter_group(self,par_group,indent=0):
        s='%stype: par_group | name: %s'%(' '*indent,par_group.name)
        print(s)

        for par in par_group.par_list:
            if isinstance(par,Parameter):
                self._show_parameter(par,indent+2)
            elif type(par)==ParameterRange:

                self._show_parameter_range(par,indent+2)
            else:
                raise RuntimeError('You can list only par or parrange from groups')
        print('')

    def _show_parameter_range(self, par_range,indent=0):
        s='%stype: par_range | name: %s'%(' '*indent,par_range.name)
        print(s)
        self._show_parameter( par_range.p1,indent+2, )
        self._show_parameter( par_range.p2,indent+2, )
        print('')

    def _show_parameter_tuple(self, par_tuple,indent=0):
        s='%stype: par_tuple | name: %s'%(' '*indent,par_tuple.name)
        print(s)
        for p in par_tuple.p_list:
            self._show_parameter( p,indent+2, )
        print('')


    def _show_parameter(self,par,indent=0):
        s='%stype: par | name: %s |  value: %s'%(' '*indent,par.name,par.value)
        print(s)

    # BUIULD DICTIONARY
    def _build_par_dictionary(self):
        self.par_dictionary_list = []
        for par in self._parameters_structure:
            self.par_dictionary_list.append({})
            if type(par) == ParameterGroup:
                self._build_parameter_group_dic(par, par_dictionary=self.par_dictionary_list[-1])

            if type(par) == ParameterRange:
                self._build_parameter_range_dic(par, par_dictionary=self.par_dictionary_list[-1])

            if isinstance(par, Parameter):
                self._build_parameter_dic(par,  par_dictionary=self.par_dictionary_list[-1])



    def _build_parameter_group_dic(self,par_group,par_dictionary=None):

        if par_dictionary is not None:
            par_dictionary['field name'] = par_group
            par_dictionary['field type'] = 'group'
            par_dictionary['object']=par_group
            par_dictionary['field value'] = []

        for par in par_group.par_list:
            #print('par',par,type(par))
            if isinstance(par,Parameter):
                val={}
                par_dictionary['field value'].append(val)
                self._build_parameter_dic(par,par_dictionary=val)

            elif isinstance(par,ParameterRange):
                val = {}
                par_dictionary['field value'].append(val)
                self._build_parameter_range_dic(par,par_dictionary=val)
            else:
                raise RuntimeError('group of parameters can contain only range of parameters or parameters')



    def _build_parameter_range_dic(self, par_range,par_dictionary=None):
        if par_dictionary is not None:
            value=[{},{}]
            par_dictionary['field name'] = par_range.name
            par_dictionary['object'] = par_range
            par_dictionary['field type'] = 'range'
            par_dictionary['field value'] = value

        self._build_parameter_dic( par_range.p1,par_dictionary=par_dictionary['field value'][0])
        self._build_parameter_dic( par_range.p2,par_dictionary=par_dictionary['field value'][1])



    def _build_parameter_dic(self,par,par_dictionary):
        if par_dictionary is not None:
            par_dictionary['field type'] = 'parameter'
            par_dictionary['object'] = par
            par_dictionary['field name'] = par.name
            par_dictionary['field value']=par.value



    def print_list(self, l):
        return l


    def print_form_dictionary_list(self,l):
        print ('type l',type(l))
        if type(l)==dict:
            if type(l['field value']) == list:
                return self.print_form_dictionary_list(l)
            else:
                print('out', l)
        elif  type(l)==list:
            print('type l', type(l))
            for d in l:
                print('type d', type(l))
                if type(d)==dict:
                    if type(d['field value'])==list:
                        print (d['field value'])
                        return  self.print_form_dictionary_list(d)
                else:
                    raise RuntimeError('should be dictionary')

        else:
            return l



    def get_parameters_list_as_json(self):
        l=[ {'query_name':self.name}]

        for par in self._parameters_list:
            l.append(par.reprJSON())


        return json.dumps(l)



class SourceQuery(BaseQuery):
    def __init__(self,name):
        RA = Angle('deg', 'RA', 0.)
        DEC = Angle('deg', 'DEC', 0.)

        sky_coords=ParameterTuple([RA,DEC],'sky_coords')

        t1_iso = Time('iso', 'T1_iso', value='2001-12-11T00:00:00.0')
        t2_iso = Time('iso', 'T2_iso', value='2001-12-11T00:00:00.0')

        t1_mjd = Time('mjd', 'T1_mjd', value=1.0)
        t2_mjd = Time('mjd', 'T2_mjd', value=1.0)

        t_range_iso = ParameterRange(t1_iso, t2_iso, 'time_range_iso')
        t_range_mjd = ParameterRange(t1_mjd, t2_mjd, 'time_range_mjd')

        time_group = ParameterGroup([t_range_iso, t_range_mjd], 'time_range', selected='t_range_iso')
        time_group_selector = time_group.build_selector('time_group_selector')


        parameters_list=[sky_coords,time_group,time_group_selector]


        super(SourceQuery, self).__init__(name,parameters_list)





class InstrumentQuery(BaseQuery):
    def __init__(self,name,
                 radius_name,
                 raidus_units,
                 radius_value,
                 E1_name,
                 E1_units,
                 E1_value,
                 E2_name,
                 E2_units,
                 E2_value,
                 input_prod_list_name=None,
                 input_prod_value=None):

        radius = Angle(raidus_units, radius_name, radius_value)


        E1_keV = SpectralBoundary(E1_units, E1_name, value=E1_value)
        E2_keV = SpectralBoundary(E2_units, E2_name, value=E2_value)

        spec_window = ParameterRange(E1_keV, E2_keV, 'spec_window')

        input_prod_list= ProdList('names_list', input_prod_list_name, value=input_prod_value)


        parameters_list=[spec_window,radius,input_prod_list]


        super(InstrumentQuery, self).__init__(name,parameters_list)







class InputProdsQuery(BaseQuery):
    def __init__(self,name, input_prod_list_name,input_prod_list ):
        super(InputProdsQuery, self).__init__(name,[ProdList('names_list', input_prod_list_name, value=input_prod_list)])




class ProductQuery(BaseQuery):
    def __init__(self,name,parameters_list,get_product_method=None,html_draw_method=None,**kwargs):
        super(ProductQuery, self).__init__(name,parameters_list, **kwargs)
        self._get_product_method = get_product_method
        self._html_draw_method = html_draw_method

    def get_product(self,instrument, config=None):
        if self._get_product_method is not None:
            return self._get_product_method(instrument,config=config)
        else:
            return None

    def get_html_draw(self, p):
        if self._html_draw_method is not None:
            return self._html_draw_method(p)
        else:
            return None

    def get_parameters_list_as_json(self):
        l=[ {'query_name':self.name},{'product_name':self.name}]

        for par in self._parameters_list:
            l.append(par.reprJSON())


        return json.dumps(l)




class Image(ProductQuery):
    def __init__(self,name,parameters_list,**kwargs):
        super(Image, self).__init__(name,parameters_list,**kwargs)


class LightCurve(ProductQuery):
    def __init__(self,name,parameters_list, **kwargs):
        super(LightCurve, self).__init__(name,parameters_list, **kwargs)


class Spectrum(ProductQuery):
    def __init__(self, name,parameters_list, **kwargs):
        super(Spectrum, self).__init__(name,parameters_list, **kwargs)




class Instrument(object):
    def __init__(self,
                 instr_name,
                 src_query,
                 instrumet_query,
                 product_queries_list=None):

        #name
        self.name=instr_name

        #src query
        self.src_query=src_query


        #Instrument specific
        self.instrumet_query=instrumet_query



        self.product_queries_list=product_queries_list

        self._queries_list=[self.src_query,self.instrumet_query]



        if product_queries_list is not None and product_queries_list !=[]:
            self._queries_list.extend(product_queries_list)

        _check_is_base_query(self._queries_list)



    def _check_names(self):
        pass

    def set_pars_from_dic(self,par_dic):
        for p, v in par_dic.items():
            print('set from form', p, v)
            self.set_par(p,v)
            print('--')

    def set_par(self,par_name,value):
        p=self.get_par_by_name(par_name)
        p.value=value




    def get_query_by_name(self,prod_name):
        p=None
        for _query in self.product_queries_list:
            if prod_name == _query.name:
                p  =  _query

        if p is None:
            raise Warning('parameter', prod_name, 'not found')

        return p


    def get_analysis_product(self,prod_name,config=None):

        return self.get_query_by_name(prod_name).get_product(self,config=config)


    def get_par_by_name(self,par_name):
        p=None

        for _query in self._queries_list:
            if par_name in _query.par_names:
                p  =  _query.get_par_by_name(par_name)

        if p is None:
            raise Warning('parameter', par_name, 'not found')

        return p



    def show_parameters_list(self):

        print ("-------------")
        for _query in self._queries_list:
            print ('q:',_query.name)
            _query.show_parameters_list()
        print("-------------")


    def get_parameters_list_as_json(self):
        l=[{'instrumet':self.name}]
        for _query in self._queries_list:
            l.append(_query.get_parameters_list_as_json())

        return l