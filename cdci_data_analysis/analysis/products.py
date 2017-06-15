

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

from .parameters import *






class AnalysisProduct(object):


    def __init__(self,_list,get_product_method=None):


        self._parameters_structure=_list
        self._parameters_list=self._build_parameters_list(_list)
        self._build_par_dictionary()
        self.product=None
        self.get_product=get_product_method

    @property
    def parameters(self):
        return self._parameters_list



    def get_par_by_name(self,name):
        p=None
        for p in self._parameters_list:
            if p.name==name:
                return p
        return p

    def set_par_value(self,name,value):
        p=self.get_par_by_name(name)
        if p is not None:
            p.value=value

    def _build_parameters_list(self,_list):
        _l=[]
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
            if isinstance(par,Parameter):
                val={}
                par_dictionary['field value'].append(val)
                self._build_parameter_dic(par,par_dictionary=val)

            elif type(par)==ParameterRange:
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

class Image(AnalysisProduct):

    def __init__(self,parameters_list,):
        super(Image, self).__init__(parameters_list)




class LightCurve(AnalysisProduct):
    pass


class Spectrum(AnalysisProduct):
    pass






