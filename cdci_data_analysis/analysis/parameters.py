"""
Overview
--------
   
general info about this module


Classes and Inheritance Structure
----------------------------------------------
.. inheritance-diagram:: 

Summary
---------
.. autosummary::
   list of the module you want
    
Module API
----------
"""

from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Andrea Tramacere"
import six
import ast
import decorator
import logging

from astropy.time import Time as astropyTime
from astropy.time import TimeDelta as astropyTimeDelta

from astropy.coordinates import Angle as astropyAngle

import numpy as np

from typing import Union

logger = logging.getLogger(__name__)


@decorator.decorator
def check_par_list(func,par_list,*args, **kwargs):
    for par in par_list:
        if isinstance(par,Parameter):
            pass
        else:
            raise RuntimeError('each parameter in the par_list has to be an instance of Parameters')

        return func(par_list, *args, **kwargs)


class ParameterGroup(object):

    def __init__(self,par_list,name,exclusive=True,def_selected=None,selected=None):
        self.name=name
        self._par_list=par_list
        self._check_pars(par_list)
        self.exclusive=True

        self.msk = np.ones(len(par_list), dtype=np.bool)

        if exclusive==True:
            self.msk[::]=False

            if def_selected is None:
                self.msk[0]==True

        if  def_selected is not None:
            self.select(def_selected)

        if selected is not None:
            self.select(selected)

    @property
    def par_list(self):
        return self._par_list

    @property
    def names(self):
        return [p.name for p in self._par_list]

    def select(self,name):
        if isinstance(name,Parameter):
            name=Parameter.value
        for ID,p in enumerate(self._par_list):
            if p.name==name:
                self.msk[ID]=True
                self._selected=self._par_list[ID].name

        if self.msk.sum()>1 and self.exclusive==True:
            raise RuntimeError('only one paramter can be selected in mutually exclusive groups')

    def _check_pars(self, par_list):
        for p in par_list:
            if isinstance(p,Parameter):
               pass
            elif isinstance(p,ParameterRange):
                pass
            else:
                raise RuntimeError('you can group Paramters or ParamtersRanges found',type(p))

    def to_list(self):
        _l=[]
        for p in self._par_list:
            if isinstance(p,Parameter):
               _l.append(p)
            elif isinstance(p,ParameterRange):
                _l.extend(p.to_list())
        return _l

    def add_par(self,par):
        self.par_list.append(par)
        self.msk=np.append(self.msk,False)

    def build_selector(self,name):
        return  Parameter(name, allowed_values=self.names)


class ParameterRange(object):

    def __init__(self,p1,p2,name):
        self._check_pars(p1,p2)
        self.name=name
        self.p1=p1
        self.p2=p2

    def _check_pars(self,p1,p2):
        if type(p1)!=type(p2):
            raise RuntimeError('pars must be of the same time')

        for p in (p1,p2):
            try:
                assert (isinstance(p,Parameter))
            except:
                raise RuntimeError('both p1 and p2 must be Parameters objects, found',type(p))

    def to_list(self):
        return [self.p1,self.p2]


class ParameterTuple(object):

    def __init__(self,p_list,name):
        self._check_pars(p_list)
        self.name=name
        self.p_list=tuple(p_list)

    def _check_pars(self,p_list):
        if any( type(x)!=type(p_list[0]) for x in p_list):
            raise RuntimeError('pars must be of the same time')

        for p in (p_list):
            try:
                assert (isinstance(p,Parameter))
            except:
                raise RuntimeError('both p1 and p2 must be Parameters objects, found',type(p))

    def to_list(self):
        return self.p_list


class Parameter(object):
    def __init__(self,
                 value=None,
                 units=None,
                 name: Union[str, None]=None, 
                 allowed_units=None,
                 default_units=None,
                 check_value=None,
                 allowed_values=None,
                 units_name=None):

        self.check_value = check_value

        if allowed_units is None:
            allowed_units = []
        else:
            allowed_units = allowed_units.copy()

        if not ( name is None or type(name) in [ str ] ):
            raise RuntimeError(f"can not initialize parameter with name {name} and type {type(name)}")

        self._allowed_units = allowed_units
        self._allowed_values = allowed_values
        self._default_units = default_units
        self.name = name
        self.units = units
        self.value = value
        self.units_name = units_name
        #self._wtform_dict=wtform_dict

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self,v):
        #print ('set',self.name,v,self._allowed_values)
        if v is not None:
            if self.check_value is not None:
                self.check_value(v, units=self.units,name=self.name)
            if self._allowed_values is not None:
                if v not in self._allowed_values:
                    raise RuntimeError('value',v,'not allowed, allowed=',self._allowed_values)
            #print('set->',self.name,v,type(v))
            if type(v)==str or isinstance(v, six.string_types):
                self._value=v.strip()
            else:
                self._value = v
        else:
            self._value=None

    @property
    def default_units(self):
        return self._default_units

    @default_units.setter
    def default_units(self, units):

        if self._allowed_units != [] and self._allowed_units is not None:
            self.chekc_units(units, self._allowed_units, self.name)

        self._default_units = units

    @property
    def units(self):
        return self._units

    @units.setter
    def units(self,units):

        if self._allowed_units !=[] and self._allowed_units is not None:

            self.chekc_units(units, self._allowed_units, self.name)

        self._units = units

    def set_value_from_form(self, form, verbose=False):
        par_name = self.name
        units_name = self.units_name
        v = None
        u = None
        in_dictionary = False

        if units_name is not None:
            if units_name in form.keys():
                u = form[units_name]

        try:
            if par_name in form.keys():
                v = form[par_name]
                in_dictionary = True
            logger.info("set_from_form: par_name=%s v=%s", par_name, v)
        except Exception as e:
            logger.error("problem e=%s setting par_name=%s, form=%s",
                         repr(e),
                         par_name,
                         form
                         )
            raise

        if in_dictionary is True:
            return self.set_par(value=v, units=u)
        else:
            if verbose is True:
                logger.debug('setting par: %s in the dictionary to its default value' % par_name )
            # set the default value
            return self.value

    def set_par(self, value, units=None):
        if units is not None:
            self.units = units
        self.value = value
        return self.get_value_in_default_format(value)

    def get_value_in_default_format(self, value):
        return value

    def get_form(self,wtform_cls,key,validators,defaults):
         return   wtform_cls('key', validators=validators, default=defaults)

    @staticmethod
    def chekc_units(units, allowed, name):
        if units not in allowed:
            # TODO this exception is not properly formatted, it could be problematic
            raise RuntimeError('wrong units for par: %s' % name, ' found: ', units, ' allowed:', allowed)

    @staticmethod
    def check_value(val,units,par_name):
        pass

    # def get_form_field(self,key=None,default=None,validators=None,wtform_dict=None,wtform=None):
    #     if key is None:
    #        key=self.name
    #
    #     if wtform is  None and wtform_dict is  None:
    #
    #         wtform_dict=self._wtform_dict
    #
    #     if default is not None:
    #         self.check_value(default,self.units)
    #     else:
    #         default=self.value
    #
    #
    #     if wtform is not None and wtform_dict is not None:
    #         raise RuntimeError('either you provide wtform or wtform_dict or you pass a wtform_dict to the constructor')
    #
    #     elif wtform_dict is not None:
    #         wtform=wtform_dict[self.units]
    #
    #     else:
    #         raise RuntimeError('yuo must provide wtform or wtform_dict')
    #
    #     return wtform(label=key, validators=validators, default=default)

    def reprJSON(self):
        return dict(name=self.name, units=self.units, value=self.value)


#class Instrument(Parameter):
#    def __init__(self,T_format,name,value=None):
        #wtform_dict = {'iso': SelectField}


class Name(Parameter):
    def __init__(self,value=None, name_format='str', name=None):
        _allowed_units = ['str']
        super(Name,self).__init__(value=value,
                                  units=name_format,
                                  check_value=self.check_name_value,
                                  name=name,
                                  allowed_units=_allowed_units)

    @staticmethod
    def check_name_value(value, units=None, name=None):
        pass


class Float(Parameter):
    def __init__(self, value=None, units=None, name=None, allowed_units=None, check_value=None):

        #wtform_dict = {'keV': FloatField}
        if check_value is None:
            check_value = self.check_float_value

        super().__init__(value=value,
                         units=units,
                         check_value=check_value,
                         name=name,
                         default_units='float',
                         allowed_units=allowed_units)
                                   #wtform_dict=wtform_dict)

        self.value=value

    @property
    def value(self):
        return self._v

    @value.setter
    def value(self, v):
        if v is not None and v!='':
            self.check_float_value(v, name=self.name, units=self.units)
            self._v = np.float(v)

        else:
            self._v=None

    def get_value_in_default_format(self, value):
        return float(value)

    @staticmethod
    def check_float_value(value, units=None, name=None):
        #print('check type of ',name,'value', value, 'type',type(value))
        if value is None or value=='':
            pass
        else:
            try:
              value=ast.literal_eval(value)
            except:
                pass
            value=np.float(value)
            if type(value) == int or type(value) == np.int:
                pass
            elif type(value) == float or type(value) == np.float:
                pass
            else:
                raise RuntimeError('type of ', name, 'not valid', type(value))


class Integer(Parameter):
    def __init__(self,value=None,units=None,name=None):

        _allowed_units = None

        #wtform_dict = {'keV': FloatField}

        super(Integer, self).__init__(value=value,
                                   units=units,
                                   check_value=self.check_int_value,
                                   name=name,
                                   allowed_units=_allowed_units)
                                   #wtform_dict=wtform_dict)

        self.value=value

    @property
    def value(self):
        return self._v

    @value.setter
    def value(self, v):
        if v is not None and v!='':
            self.check_int_value(v,name=self.name)
            self._v = np.int(v)

        else:
            self._v=None


    @staticmethod
    def check_int_value(value, units=None,name=None):
        #print('check type of ',name,'value', value, 'type',type(value))
        if value is None or value=='':
            pass
        else:
            try:
              value=ast.literal_eval(value)
            except:
                pass
            value=np.int(value)
            if type(value) == int or type(value) == np.int:
                pass
            elif type(value) == float or type(value) == np.float:
                pass
            else:
                raise RuntimeError('type of ', name, 'not valid', type(value))


class Time(Parameter):
    def __init__(self, value=None, T_format='isot', name=None, Time_format_name=None):

        #_allowed_units = astropyTime.FORMATS

        #wtform_dict = {'iso': StringField}
        #wtform_dict['mjd'] = FloatField
        #wtform_dict['prod_list'] = TextAreaField

        super(Time, self).__init__(value=value,
                                   units=T_format,
                                   units_name=Time_format_name,
                                   default_units='isot',
                                   name=name,
                                   allowed_units=None)
                                  #wtform_dict=wtform_dict)

        self._set_time(value, format=T_format)

    def get_value_in_default_format(self, value) -> Union[str, float, None]:
        return getattr(self._astropy_time, self.default_units, )

    @property
    def value(self):
        return self._astropy_time.value

    @value.setter
    def value(self, v):
      
        units=self.units
        self._set_time(v, format=units)

    def _set_time(self, value, format):
       
        try:
            value=ast.literal_eval(value)
        except:
            pass
        
        self._astropy_time = astropyTime(value, format=format)
        
        self._value =value


class TimeDelta(Parameter):
    def __init__(self, value=None, delta_T_format='sec', name=None, delta_T_format_name=None):

        # _allowed_units = astropyTime.FORMATS

        # wtform_dict = {'iso': StringField}
        # wtform_dict['mjd'] = FloatField
        # wtform_dict['prod_list'] = TextAreaField

        super(TimeDelta, self).__init__(value=value,
                                   units=delta_T_format,
                                   units_name=delta_T_format_name,
                                   name=name,
                                   allowed_units=None)
        # wtform_dict=wtform_dict)


        self._set_time(value, format=delta_T_format)

    @property
    def value(self):
        return self._astropy_time_delta.value

    @value.setter
    def value(self, v):

        units = self.units
        self._set_time(v, format=units)

    def _set_time(self, value, format):

        try:
            value = ast.literal_eval(value)
        except:
            pass

        #print ('value',value)
        self._astropy_time_delta = astropyTimeDelta(value, format=format)

        self._value = value

class InputProdList(Parameter):
    def __init__(self, value=None, _format='names_list', name: str=None):
        _allowed_units = ['names_list']

        if value is None:
            value=[]

        super(InputProdList, self).__init__(value=value,
                                            units=_format,
                                            check_value=self.check_list_value,
                                            name=name,
                                            allowed_units=_allowed_units)
                                  #wtform_dict=wtform_dict)

        self._split(value)


    def _split(self,str_list):

        if type(str_list) == list:
               pass
        elif type(str_list) == str or type(str(str_list)) == str:
            if ',' in str_list:
                str_list = str_list.split(',')
            else:
                str_list = str_list.split(' ')
        else:
           raise RuntimeError('parameter format is not correct')

        if str_list == ['']:
            str_list = []

        return str_list

    @property
    def value(self):
        if self._value==[''] or self._value is None:
            return []
        else:
            return self._value

    @value.setter
    def value(self, v):
        #print('set', self.name, v, self._allowed_values)
        if v is not None:
            if self.check_value is not None:
                self.check_value(v, units=self.units, name=self.name)
            if self._allowed_values is not None:
                if v not in self._allowed_values:
                    raise RuntimeError('value', v, 'not allowed, allowed=', self._allowed_values)
            if v == [''] or v is None or str(v) == '':
                self._value=['']
            else:
                self._value = v
        else:
            self._value = ['']
        self._value=self._split(self._value)
        #print ('set to ',self._value)


    @staticmethod
    def check_list_value(value,units,name='par'):
        if units=='names_list':
            try:
                #print(type(value))
                assert (type(value) == list or type(value) == str  or type(str(value))== str)
            except:
                raise RuntimeError('par:',name,', value is not product list format : list of strings','it is',type(value),value)
        else:
            raise  RuntimeError(name,'units not valid',units)






class Angle(Parameter):
        def __init__(self,value=None, units=None,name=None):

            super(Angle, self).__init__(value=value,
                                       units=units,
                                       name=name,
                                       allowed_units=None)
            # wtform_dict=wtform_dict)


            self._set_angle(value, units=units)

        @property
        def value(self):
            return self._astropy_angle.value

        @value.setter
        def value(self, v, units=None):
            if units is None:
                units = self.units



            self._set_angle(v, units=units)

        def _set_angle(self, value, units):
            if value=='' or value is None:
                pass
            else:
                self._astropy_angle = astropyAngle(value, unit=units)
                self._value = self._astropy_angle.value

# class AngularDistance(Parameter):
#     def __init__(self, angular_units,name, value=None):
#         _allowed_units = ['deg']
#         super(AngularDistance, self).__init__(value=value,
#                                      units=angular_units,
#                                      check_value=self.check_angle_value,
#                                      name=name,
#                                      allowed_units=_allowed_units)
#
#
#
#     @staticmethod
#     def check_angle_value(value, units=None, name=None):
#         print('check type of ', name, 'value', value, 'type', type(value))
#         pass
#


class SpectralBoundary(Float):
    def __init__(self, value=None, E_units='keV' ,name=None):

        _allowed_units = ['keV', 'eV', 'MeV', 'GeV', 'TeV', 'Hz', 'MHz', 'GHz']

        #wtform_dict = {'keV': FloatField}

        super().__init__(value=value,
                         units=E_units,
                         check_value=self.check_energy_value,
                         name=name,
                         allowed_units=_allowed_units)
                           #wtform_dict=wtform_dict)

    @staticmethod
    def check_energy_value(value, units=None,name=None):
        #print('check type of ',name,'value', value, 'type',type(value))

        try:
            value=ast.literal_eval(value)
        except:
            pass

        if type(value)==int or type(value)==np.int:
            pass
        elif type(value)==float or type(value)==np.float:
            pass
        else:
            raise RuntimeError('type of ',name,'not valid',type(value))


class Energy(Float):
    def __init__(self,value=None,E_units=None,name=None):

        _allowed_units = ['keV', 'eV', 'MeV', 'GeV', 'TeV']

        #wtform_dict = {'keV': FloatField}

        super(Energy, self).__init__(value=value,
                                   units=E_units,
                                   check_value=self.check_energy_value,
                                   name=name,
                                   allowed_units=_allowed_units)
                                   #wtform_dict=wtform_dict)

    @staticmethod
    def check_energy_value(value, units=None,name=None):
        #print('check type of ',name,'value', value, 'type',type(value))

        try:
            value=ast.literal_eval(value)
        except:
            pass

        if type(value)==int or type(value)==np.int:
            pass
        elif type(value)==float or type(value)==np.float:
            pass
        else:
            raise RuntimeError('type of ',name,'not valid',type(value))


class DetectionThreshold(Float):
    def __init__(self,value=None,units='sigma',name=None):

        _allowed_units = ['sigma']

        #wtform_dict = {'keV': FloatField}

        super(DetectionThreshold, self).__init__(value=value,
                                   units=units,
                                   check_value=self.check_value,
                                   name=name,
                                   allowed_units=_allowed_units)
                                   #wtform_dict=wtform_dict)
    # TODO this method, behaves exaclty the same as the one for the Energy and SpectralBoundary classes
    # is this ever expected to be more specific?
    @staticmethod
    def check_value(value, units=None,name=None):
        #print('check type of ',name,'value', value, 'type',type(value))


        try:
            value=ast.literal_eval(value)
        except:
            pass

        if type(value)==int or type(value)==np.int:
            pass
        elif type(value)==float or type(value)==np.float:
            pass
        else:
            raise RuntimeError('type of ',name,'not valid',type(value))



class UserCatalog(Parameter):
    def __init__(self, value=None,name_format='str', name=None):
        _allowed_units = ['str']
        super(UserCatalog,self).__init__(value=value,
                                  units=name_format,
                                  check_value=self.check_name_value,
                                  name=name,
                                  allowed_units=_allowed_units)

    @staticmethod
    def check_name_value(value, units=None, name=None):
        pass
