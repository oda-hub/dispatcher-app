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
import typing

__author__ = "Andrea Tramacere"

# TODO importing six necessary for compatibility with the plugin, to be removed in the future with the necessary adaptations
import six
import decorator
import logging
import os

from astropy.time import Time as astropyTime
from astropy.time import TimeDelta as astropyTimeDelta
from astropy import units as apy_u 

import numpy as np

from typing import Union
from inspect import signature
from inspect import Parameter as call_parameter
from .exceptions import RequestNotUnderstood

from jsonschema import validate, ValidationError, SchemaError
import json
from cdci_data_analysis import conf_dir

logger = logging.getLogger(__name__)


@decorator.decorator
def check_par_list(func, par_list, *args, **kwargs):
    for par in par_list:
        if isinstance(par, Parameter):
            pass
        else:
            raise RuntimeError('each parameter in the par_list has to be an instance of Parameters')

        return func(par_list, *args, **kwargs)

def subclasses_recursive(cls):
    direct = cls.__subclasses__()
    indirect = []
    for subclass in direct:
        indirect.extend(subclasses_recursive(subclass))
    return direct + indirect

# TODO this class seems not to be in use anywhere, not even the plugins
class ParameterGroup(object):

    def __init__(self, par_list, name, exclusive=True, def_selected=None, selected=None):
        self.name = name
        self._par_list = par_list
        self._check_pars(par_list)
        self.exclusive = True

        self.msk = np.ones(len(par_list), dtype=bool)

        if exclusive:
            self.msk[::] = False

            if def_selected is None:
                self.msk[0] = True

        if def_selected is not None:
            self.select(def_selected)

        if selected is not None:
            self.select(selected)

    @property
    def par_list(self):
        return self._par_list

    @property
    def names(self):
        return [p.name for p in self._par_list]

    def select(self, name):
        if isinstance(name, Parameter):
            name = Parameter.value
        for ID, p in enumerate(self._par_list):
            if p.name == name:
                self.msk[ID] = True
                self._selected = self._par_list[ID].name

        if self.msk.sum() > 1 and self.exclusive == True:
            raise RuntimeError('only one paramter can be selected in mutually exclusive groups')

    def _check_pars(self, par_list):
        for p in par_list:
            if isinstance(p, Parameter):
                pass
            elif isinstance(p, ParameterRange):
                pass
            else:
                raise RuntimeError('you can group Paramters or ParamtersRanges found', type(p))

    def to_list(self):
        _l = []
        for p in self._par_list:
            if isinstance(p, Parameter):
                _l.append(p)
            elif isinstance(p, ParameterRange):
                _l.extend(p.to_list())
        return _l

    def add_par(self, par):
        self.par_list.append(par)
        self.msk = np.append(self.msk, False)

    def build_selector(self, name):
        return Parameter(name, allowed_values=self.names)


class ParameterRange(object):

    def __init__(self, p1, p2, name):
        self._check_pars(p1, p2)
        self.name = name
        self.p1 = p1
        self.p2 = p2

    def _check_pars(self, p1, p2):
        if type(p1) != type(p2):
            raise RuntimeError('pars must be of the same type')
        if not isinstance(p1, Parameter) or not isinstance(p2, Parameter):
            raise RuntimeError(
                f'both p1 and p2 must be Parameters objects, found {type(p1).__name__} for p1 and {type(p2).__name__} for p2')

    def to_list(self):
        return [self.p1, self.p2]


class ParameterTuple:

    def __init__(self, p_list, name):
        self._check_pars(p_list)
        self.name = name
        self.p_list = tuple(p_list)

    def _check_pars(self, p_list):
        for p in p_list:
            if not isinstance(p, Parameter):
                raise RuntimeError(f'all the members of the tuple must be Parameters instances, found a {type(p).__name__}')

            if not isinstance(p, type(p_list[0])):
                raise RuntimeError('pars must be of the same type')

    def to_list(self):
        return self.p_list


class Parameter:
    """
    # General notes

    format:
    * Every `Parameter` has a *format*. *Format* defines representation of the parameter value in parameter URL, analysis_parameters dictionary, oda_api, and embedded in fits files.       
    * Parameter dictionary may include format specifiers for each parameter.  If non-default format is used the parameter, parameter format specifier is required. If not specified, the default is used.
    * *default format* of defines **unique** representation of the parameter. Any parameter value can be converted to default *format*. By using default parameter representations, it is possible to construct unique and deterministic request parameter dictionaries, URLs, oda_api codes.
    
    unit:
    * physical quantities are represented as floats (and should inherit from `Float`) and are scaled with *units*. The same value may be represented as different floats if units are correspondingly different.
    * units are treated similarly to formats: there are unit specifiers and default units. 
    
    type:
    * each parameter is constructed with a value of some types. Types is verified at construction, and converted to default type. Further representations are defined by units and formats
    
    TODO see through that this is implemented https://github.com/oda-hub/dispatcher-app/issues/339
    """
    def __init__(self,
                 value,
                 units=None,
                 name: Union[str, None] = None,
                 # TODO should we remove units/type/format knowledge from the Parameter class?
                 allowed_units=None,
                 default_units=None,
                 units_name=None,

                 par_format=None,
                 par_default_format=None,
                 par_format_name=None,

                 default_type=None,
                 allowed_types=None,

                 check_value=None,
                 allowed_values=None,
                 force_default_value=False,
                 min_value=None,
                 max_value=None,
                 is_optional=False,

                 extra_metadata=None,
                 **kwargs
                 ):
        
        if len(kwargs) > 0:
            logger.error("possibily programming error: class %s initialized with extra arguments %s",
                         self, kwargs)
        
        if (units is None or units == '') and \
                default_units is not None and default_units != '':
            # TODO ideally those should be renamed as singular (unit and default_unit)
            #  but they are as such because they're used also in plugins
            #
            units = default_units

        if (par_format is None or par_format == '') and \
                par_default_format is not None and par_default_format != '':
            par_format = par_default_format

        if check_value is not None:
            logger.warning('Passing check_value to class constructor is deprecated. Override .check_value() method instead.')
            self._deprecated_check_value = check_value

        if allowed_units is not None:
            # handles case of []
            if not allowed_units:
                logger.warning("an empty list for the allowed_units is considered as None")
                allowed_units = None
            else:
                allowed_units = allowed_units.copy()

        if allowed_types is not None:
            allowed_types = allowed_types.copy()

        # if not (name is None or type(name) in [str]):
        #     raise RuntimeError(f"can not initialize parameter with name {name} and type {type(name)}")

        self.is_optional = is_optional
        self._allowed_units = allowed_units
        self._allowed_values = allowed_values
        self.force_default_value = force_default_value
        self._allowed_types = allowed_types
        self.name = name
        self.default_units = default_units
        self.units = units
        self.units_name = units_name
        self.default_type = default_type
        self.par_format=par_format
        self.par_default_format=par_default_format
        self.par_format_name=par_format_name
        self._min_value = min_value
        self._max_value = max_value
        self._bound_units = self.units
        self.value = value
        self.extra_metadata = extra_metadata


        self._arg_list = [self.name]
        if par_format_name is not None:
            self._arg_list.append(par_format_name)
        if units_name is not None:
            self._arg_list.append(units_name)
        
    @property
    def argument_names_list(self):
        return self._arg_list[:]

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, par_name):
        if not (par_name is None or type(par_name) in [str]):
            raise RuntimeError(f"can not initialize parameter with name {par_name} and type {type(par_name).__name__}")
        self._name = par_name

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, v):
        if v is not None:
            if isinstance(v, str):
                v = v.strip()

            try:
                self.set_par_internal_value(v)
            except ValueError as e:
                raise RequestNotUnderstood(f'Parameter {self.name} wrong value {v}. {e}')
            except Exception:
                raise

            self.check_value()
            
            if self._deprecated_check_value is not None:
                kwargs = { kw: getattr(self, kw) for kw in ('units', 'name', 'par_format') 
                          if kw in signature(self._deprecated_check_value).parameters }        
                self._deprecated_check_value(v, **kwargs)
           
            if self._min_value is not None or self._max_value is not None:
                self.check_bounds()
                
            if self._allowed_values is not None:
                if v not in self._allowed_values:
                    raise RequestNotUnderstood(f'Parameter {self.name} wrong value {v}: not in allowed {self._allowed_values}')
        else:
            if not self.is_optional:
                raise RequestNotUnderstood(f'Non-optional parameter {self.name} is set to None')
            self._value = None

    def set_par_internal_value(self, value):
        # This method may be overrided and used to set both self._value and internal units- or format- aware representation
        self._value = value

    @property
    def default_units(self):
        return self._default_units

    @default_units.setter
    def default_units(self, par_unit):
        if par_unit is not None and self._allowed_units is not None:
            self.check_units(par_unit, self._allowed_units, self.name)

        self._default_units = par_unit

    @property
    def default_type(self):
        return self._default_type

    @default_type.setter
    def default_type(self, par_type):
        if par_type is not None and self._allowed_types is not None:
            self.check_type(par_type, self._allowed_types, self.name)

        self._default_type = par_type

    @property
    def units(self):
        return self._units

    @units.setter
    def units(self, units):
        if units is None and self.default_units is not None:
            logger.warning(f'Units not set for {self.name}, using default units: {self.default_units}')
            units = self.default_units
        if units is not None and self._allowed_units is not None:
            self.check_units(units, self._allowed_units, self.name)

        self._units = units

    def set_value_from_form(self, form, verbose=False):
        par_name = self.name
        units_name = self.units_name
        par_format_name = self.par_format_name
        v = None
        u = None
        f = None
        in_dictionary = False
        # there is either a units or a format
        if units_name is not None:
            if units_name in form.keys():
                u = form[units_name]
        if par_format_name is not None:
            if par_format_name in form.keys():
                f = form[par_format_name]

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
            return self.set_par(value=v, units=u, par_format=f)
        elif self.force_default_value:
            return self.set_par(value=self.get_default_value(), units=u, par_format=f)
        else:
            if verbose is True:
                logger.debug('setting par: %s in the dictionary to its default value' % par_name)
            # set the default value
            return self.value

    def set_par(self, value, units=None, par_format=None):
        if units is not None:
            self.units = units

        if par_format is not None:
            self.par_format = par_format

        self.value = value
        return self.get_default_value()

    def get_default_value(self):
        return self.value

    def get_value_in_default_format(self):
        return self.get_value_in_format(self.par_default_format)

    def get_value_in_format(self, format):
        logger.warning(f'no explict conversion implemented for the parameter {self.name}, '
                       f'the non converted value is returned')
        return self.value
    
    def get_value_in_default_units(self):
        return self.get_value_in_units(self.default_units)

    def get_value_in_units(self, units):
        logger.warning(f'no explict conversion implemented for the parameter {self.name}, '
                       f'the non converted value is returned')
        return self.value

    def get_form(self, wtform_cls, key, validators, defaults):
        return wtform_cls('key', validators=validators, default=defaults)

    def chekc_units(self, *args, **kwargs):
        logger.warning('please update to new interface! -- ....')
        return self.check_units(*args, **kwargs)

    @staticmethod
    def check_units(units, allowed, name):
        if units not in allowed:
            raise RuntimeError(f'wrong units for par: {name}, found: {units}, allowed: {allowed}')

    @staticmethod
    def check_type(par_type, allowed, name):
        if par_type not in allowed:
            raise RuntimeError(f'wrong type for par: {name}, found: {par_type}, allowed: {allowed}')

    @staticmethod
    def _deprecated_check_value(val, units=None, name=None, par_format=None):
        pass
    
    def check_bounds(self):
        raise NotImplementedError(f"Parameter {self.name} doesn't support min_value/max_value check")
    
    def check_value(self):
        pass
            
    def reprJSONifiable(self):
        # produces json-serialisable list
        reprjson = [dict(name=self.name, units=self.units, value=self.value)]
        restrictions = {'is_optional': self.is_optional}
        if self._allowed_values is not None:
            restrictions['allowed_values'] = self._allowed_values
        if getattr(self, '_min_value', None) is not None:
            restrictions['min_value'] = self._min_value
        if getattr(self, '_max_value', None) is not None:
            restrictions['max_value'] = self._max_value
        if getattr(self, 'schema', None) is not None:
            restrictions['schema'] = self.schema
        if restrictions:
            reprjson[0]['restrictions'] = restrictions
        if getattr(self, 'extra_metadata', None) is not None:
            reprjson[0]['extra_metadata'] = self.extra_metadata
        if getattr(self, 'owl_uris', None):
            if isinstance(self.owl_uris, str):
                reprjson[0]['owl_uri'] = [ self.owl_uris ]
            elif isinstance(self.owl_uris, tuple):
                reprjson[0]['owl_uri'] = list(self.owl_uris)
            else:
                reprjson[0]['owl_uri'] = self.owl_uris
        if self.par_format_name is not None:
            reprjson.append(dict(name=self.par_format_name, units="str", value=self.par_format))
        return reprjson

    @classmethod
    def matches_owl_uri(cls, owl_uri: str) -> bool:
        return owl_uri in getattr(cls, "owl_uris", ())

    @classmethod
    def from_owl_uri(cls,
                     owl_uri,
                     extra_ttl = None,
                     ontology_path = None, 
                     ontology_object = None,
                     **kwargs):
        from oda_api.ontology_helper import Ontology

        metadata_keys = ['label', 'description', 'group']

        if ontology_path is not None and ontology_object is not None:
            raise RuntimeError("Both ontology_path and ontology_object parameters are set.")
        elif ontology_path is None and ontology_object is None:
            logger.warning('Ontology path/object not set in Parameter.from_owl_uri(). '
                'Trying to find parameter which have %s directly set. '
                'extra_ttl will be ignored ', owl_uri)
            parameter_hierarchy = [ owl_uri ]
            par_format = par_unit = allowed_values = min_value = max_value = label = description = group = None
        else:
            if ontology_path is not None:
                if isinstance(ontology_path, (str, os.PathLike)):
                    onto = Ontology(ontology_path)
                else:
                    raise RuntimeError("Wrong ontology_path")
            else:
                if isinstance(ontology_object, Ontology):
                    onto = ontology_object
                else:
                    raise RuntimeError("Wrong ontology_object")
            
            if extra_ttl is not None:
                onto.parse_extra_triples(extra_ttl)
            parameter_hierarchy = onto.get_parameter_hierarchy(owl_uri)
            par_format = onto.get_parameter_format(owl_uri)
            par_unit = onto.get_parameter_unit(owl_uri)
            min_value, max_value = onto.get_limits(owl_uri)
            allowed_values = onto.get_allowed_values(owl_uri)
            label = onto.get_direct_annotation(owl_uri, "label")
            description = onto.get_direct_annotation(owl_uri, "description")
            group = onto.get_direct_annotation(owl_uri, "group")

        for owl_superclass_uri in parameter_hierarchy:
            for python_subclass in subclasses_recursive(cls):
                logger.debug("searching for class with owl_uri=%s, found %s", owl_superclass_uri, python_subclass)
                if python_subclass.matches_owl_uri(owl_superclass_uri):
                    logger.info("will construct %s by owl_uri %s", python_subclass, owl_superclass_uri)
                    call_kwargs = {
                        'extra_metadata': {key: val for key, val in zip(metadata_keys, [label, description, group]) if
                                           val is not None}}
                    call_signature = signature(python_subclass)
                    var_kw_signature = call_parameter.VAR_KEYWORD in [x.kind for x in call_signature.parameters.values()]
                    
                    for restr, overr_kw, kw_name in [(par_format, 'format_kw', 'par_format'), 
                                                     (par_format, 'default_format_kw', 'par_default_format'),
                                                     (par_unit, 'units_kw', 'units'), 
                                                     (par_unit, 'default_units_kw', 'default_units'),
                                                     (min_value, 'notexist', 'min_value'), 
                                                     (max_value, 'notexist', 'max_value'),
                                                     (allowed_values, 'notexist', 'allowed_values')]:
                        if restr is not None:
                            par_kw = getattr(python_subclass, overr_kw, kw_name)
                            if var_kw_signature or par_kw in call_signature.parameters:
                                call_kwargs[par_kw] = restr
                            else:
                                logger.error(("according to ontology, owl_uri %s parameter have %s=%s "
                                            "but %s doesn't have such keyword, "
                                            "so it will be discarded for the instantiation"),
                                            owl_uri, par_kw, restr, python_subclass)
                    
                    for par_name, par_value in kwargs.items():
                        if var_kw_signature or par_name in call_signature.parameters:
                            if par_name in call_kwargs.keys(): 
                                logger.warning("overriding ontology-derived value of the %s keyword of %s with explicitly set value %s",
                                               par_name, python_subclass, par_value)
                            call_kwargs[par_name] = par_value
                        else:
                            logger.error(("parameter %s with value %s not used to construct %s "
                                          "and will be discarded for the instantiation, available parameters %s"),
                                        par_name, par_value, python_subclass, call_signature)
                    try:
                        parameter_interpretation = python_subclass(**call_kwargs)
                        return parameter_interpretation
                    except Exception as e:
                        logger.exception(("owl_uri %s matches Parameter %s, but the Parameter constructor failed! "
                                          "Possibly a programming error"), 
                                         owl_superclass_uri, python_subclass)

        logger.warning(('Unknown owl type uri %s or failed to construct any parameter. '
                        'Creating basic Parameter object.'), owl_uri)
        return cls(**kwargs)
    
class String(Parameter):
    owl_uris = ("http://www.w3.org/2001/XMLSchema#str", "http://odahub.io/ontology#String")
    
    def __init__(self, value, name_format='str', name=None, allowed_values = None, force_default_value=False, is_optional=False, extra_metadata = None):

        _allowed_units = ['str']
        super().__init__(value=value,
                         units=name_format,
                         check_value=self.check_name_value,
                         name=name,
                         allowed_units=_allowed_units,
                         allowed_values=allowed_values,
                         force_default_value=force_default_value,
                         is_optional=is_optional,
                         extra_metadata=extra_metadata)

    @staticmethod
    def check_name_value(value, units=None, name=None, par_format=None):
        pass

class LongString(String):
    owl_uris = String.owl_uris + ("http://odahub.io/ontology#LongString",)

class Name(String):
    owl_uris = String.owl_uris + ("http://odahub.io/ontology#AstrophysicalObject",)

class FileReference(String):
    owl_uris = String.owl_uris + ("http://odahub.io/ontology#FileReference",)

class POSIXPath(FileReference):
    owl_uris = FileReference.owl_uris + ("http://odahub.io/ontology#POSIXPath",)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, force_default_value=True, **kwargs)

    def get_default_value(self):
        return ''

class FileURL(FileReference):
    owl_uris = FileReference.owl_uris + ("http://odahub.io/ontology#FileURL",)

class NumericParameter(Parameter):
    owl_uris = ("http://odahub.io/ontology#NumericParameter")

    def __init__(self, *args, **kwargs):
        if kwargs.get('allowed_types') is None:
            kwargs['allowed_types'] = [int, float]
        
        if kwargs.get('default_type') is None:
            val = kwargs.get('value', args[0])
            if type(val) in kwargs['allowed_types']:
                kwargs['default_type'] = type(val)
            else:    
                for tp in kwargs['allowed_types']:
                    try:
                        tp(val)
                        kwargs['default_type'] = tp
                        break
                    except ValueError:
                        continue
                if kwargs.get('default_type') is None:
                    kwargs['default_type'] = float # fallback, should fail on check
                
        
        super().__init__(*args, **kwargs)
            
    
    def set_par_internal_value(self, value):
        if value is not None and value != '':
            self._value = self.default_type(value)
            if self.units is not None:
                u = getattr(apy_u, self.units)
                self._quantity = self._value * u
            else:
                self._quantity = self._value
        else:
            self._value = None
            self._quantity = None
    
    def get_value_in_units(self, units):
        if self.value is None:
            return None
        if units is None:
            return self.value
        if self._quantity is None:
            return None
        u = getattr(apy_u, units)
        return self._quantity.to_value(u)
    
    def get_default_value(self):
        return self.get_value_in_default_units()
    
    def check_bounds(self):
        if self._min_value is not None:
            min_value = float(self._min_value)
            if self.get_value_in_units(self._bound_units) < min_value:
                raise RequestNotUnderstood((f'Parameter {self.name} wrong value '
                                            f'{self.get_value_in_units(self._bound_units)}'
                                            f'{self._bound_units if self._bound_units is not None else ""}: '
                                            f'should be greater or equal than {min_value}'))
        if self._max_value is not None:
            max_value = float(self._max_value)
            if self.get_value_in_units(self._bound_units) > max_value:
                raise RequestNotUnderstood((f'Parameter {self.name} wrong value '
                                            f'{self.get_value_in_units(self._bound_units)}'
                                            f'{self._bound_units if self._bound_units is not None else ""}: '
                                            f'should be lower or equal than {max_value}'))

class Float(NumericParameter):
    owl_uris = ("http://www.w3.org/2001/XMLSchema#float", "http://odahub.io/ontology#Float")
    def __init__(self, 
                 value, 
                 units=None, 
                 name=None, 
                 allowed_units=None, 
                 default_units=None, 
                 check_value=None, 
                 min_value= None,
                 max_value = None,
                 units_name = None, 
                 is_optional=False,
                 extra_metadata=None):
       
        super().__init__(value=value,
                         units=units,
                         check_value=check_value,
                         default_units=default_units,
                         name=name,
                         default_type=float,
                         # TODO added for consistency with Integer
                         allowed_types=[float],
                         allowed_units=allowed_units,
                         min_value=min_value,
                         max_value=max_value,
                         units_name = units_name,
                         is_optional=is_optional,
                         extra_metadata=extra_metadata)



class Integer(NumericParameter):
    owl_uris = ("http://www.w3.org/2001/XMLSchema#int", "http://odahub.io/ontology#Integer")

    def __init__(self, 
                 value, 
                 units=None, 
                 name=None, 
                 check_value=None, 
                 min_value = None, 
                 max_value = None,
                 units_name = None,
                 default_units = None,
                 allowed_units = None,
                 is_optional=False,
                 extra_metadata=None):
        
        super().__init__(value=value,
                         units=units,
                         default_units = default_units,
                         check_value=check_value,
                         default_type=int,
                         allowed_types=[int],
                         name=name,
                         allowed_units=allowed_units,
                         min_value = min_value,
                         max_value = max_value,
                         units_name = units_name,
                         is_optional=is_optional,
                         extra_metadata=extra_metadata)

    def set_par_internal_value(self, value):
        if isinstance(value, float):
            message = f'{value} is an invalid value for {self.name} since it cannot be used as an Integer'
            logger.error(message)
            raise RequestNotUnderstood(message)
        return super().set_par_internal_value(value)

class Time(Parameter):
    owl_uris = ("http://odahub.io/ontology#TimeInstant",)
    format_kw = 'T_format'
    
    def __init__(self, 
                 value, 
                 T_format='isot', 
                 name=None, 
                 Time_format_name='T_format', 
                 par_default_format='isot',
                 is_optional=False, 
                 extra_metadata=None):

        super().__init__(value=value,
                         par_format=T_format,
                         par_format_name=Time_format_name,
                         par_default_format=par_default_format,
                         name=name,
                         is_optional=is_optional,
                         extra_metadata=extra_metadata)

    def get_default_value(self):
        return self.get_value_in_default_format()

    def get_value_in_format(self, par_format):
        if self.value is None:
            return None
        return getattr(self._astropy_time, par_format)

    @property
    def units(self):
        # for backward compatibility
        return self.par_format

    @units.setter
    def units(self, units):
        # for backward compatibility
        if units is not None and self._allowed_units is not None:
            self.check_units(units, self._allowed_units, self.name)

        self.par_format = units

    @property
    def value(self):
        return self._astropy_time.value
    
    @value.setter
    def value(self, v):
        super(self.__class__, self.__class__).value.fset(self, v)

    def set_par_internal_value(self, value):
        if value is None:
            self._value = None
            return
        try:
            self._astropy_time = astropyTime(value, format=self.par_format)
        except ValueError as e:
            raise RequestNotUnderstood(f'Parameter {self.name} wrong value {value}: can\'t be parsed as Time of {self.par_format} format')
        self._value = value


# TODO: redefine time-timedelta relation
# it is confusing that TimeDelta derives from Time.  
# https://github.com/astropy/astropy/blob/main/astropy/time/core.py#L379
# NOTE: added deprecation warning and introduced TimeInterval 
class TimeDelta(Time):
    owl_uris = ("http://odahub.io/ontology#TimeDeltaIsDeprecated",) 
    format_kw = 'delta_T_format'
    
    def __init__(self, 
                 value, 
                 delta_T_format='sec', 
                 name=None, 
                 delta_T_format_name=None, 
                 par_default_format='sec', 
                 extra_metadata=None):
        logging.warning(('TimeDelta parameter is deprecated. '
                         'It derives from Time, which is confusing. '
                         'Consider using TimeInterval parameter.'))
        super().__init__(value=value,
                         T_format=delta_T_format,
                         Time_format_name=delta_T_format_name,
                         par_default_format=par_default_format,
                         name=name,
                         extra_metadata=extra_metadata)

    def get_value_in_format(self, units):
        return getattr(self._astropy_time_delta, units)

    @property
    def value(self):
        return self._astropy_time_delta.value

    @value.setter
    def value(self, v):
        units = self.units
        self._set_time(v, format=units)

    def _set_time(self, value, format):
        if value is None:
            self._value = None
            return
        try:
            self._astropy_time_delta = astropyTimeDelta(value, format=format)
        except ValueError as e:
            raise RequestNotUnderstood(f'Parameter {self.name} wrong value {value}: can\'t be parsed as TimeDelta of {format} format')

        self._value = value

class TimeInterval(Float):
    owl_uris = ("http://odahub.io/ontology#TimeInterval",) 
    
    def __init__(self, 
                 value, 
                 units='s', 
                 name=None, 
                 default_units='s', 
                 min_value=None, 
                 max_value=None,
                 units_name = None,
                 is_optional=False,
                 extra_metadata=None):

        _allowed_units = ['s', 'minute', 'hour', 'day', 'year']
        super().__init__(value=value,
                         units=units,
                         default_units=default_units,
                         name=name,
                         min_value=min_value,
                         max_value=max_value,
                         units_name = units_name,
                         allowed_units=_allowed_units,
                         is_optional=is_optional,
                         extra_metadata=extra_metadata)

class InputProdList(Parameter):
    owl_uris = ('http://odahub.io/ontology#InputProdList',)
    
    # TODO removal of the leading underscore cannot be done for compatibility with the plugins
    def __init__(self, value=None, _format='names_list', name: str = None, extra_metadata=None):
        _allowed_units = ['names_list']

        if value is None:
            value = []

        super().__init__(value=value,
                         par_format=_format,
                         check_value=self.check_list_value,
                         name=name,
                         allowed_units=_allowed_units,
                         is_optional=True,
                         extra_metadata=extra_metadata)

    @staticmethod
    def _split(str_list):
        if type(str_list) == list:
            pass
        elif isinstance(str_list, str) or isinstance(str(str_list), str):
            str_list = str(str_list)
            str_list = str_list.split(',')
        else:
            raise RuntimeError('parameter format is not correct')

        if str_list == ['']:
            str_list = []

        return str_list

    @property
    def value(self):
        if self._value == [''] or self._value is None:
            return []
        else:
            return self._value

    @value.setter
    def value(self, v):
        if v is not None:
            if self._deprecated_check_value is not None:
                self._deprecated_check_value(v, par_format=self.par_format, name=self.name)
            if self._allowed_values is not None:
                if v not in self._allowed_values:
                    raise RequestNotUnderstood(f'Parameter {self.name} wrong value {v}: not in allowed {self._allowed_values}')
            if v == [''] or v is None or str(v) == '':
                self._value = ['']
            else:
                self._value = v
        else:
            self._value = ['']
        self._value = self._split(self._value)
        # print ('set to ',self._value)

    @staticmethod
    def check_list_value(value, units=None, name=None, par_format=None):
        if par_format == 'names_list':
            # TODO the condition 'isinstance(str(value), str))' was quite unclear to me, and probably useless since could lead to unexpected behavior
            if not isinstance(value, (list, str, float, int)):
                raise RuntimeError(f'value of the parameter {name} is not a valid product list format, but {type(value).__name__} has been found')
        else:
            raise RuntimeError(f'{name} units not valid {par_format}')


class Angle(Float):
    owl_uris = ("http://odahub.io/ontology#Angle")
    
    def __init__(self, 
                 value, 
                 units=None, 
                 default_units='deg', 
                 name=None, 
                 min_value = None, 
                 max_value = None,
                 units_name = None,
                 is_optional=False,
                 extra_metadata=None):

        super().__init__(value=value,
                         units=units,
                         # TODO can we safely make this assumption?
                         default_units=default_units,
                         name=name,
                         allowed_units=None,
                         min_value = min_value,
                         max_value = max_value,
                         units_name = units_name,
                         is_optional=is_optional,
                         extra_metadata=extra_metadata)


class Energy(Float):
    owl_uris = ("http://odahub.io/ontology#Energy", "http://odahub.io/ontology#Frequency")
    units_kw = 'E_units'
    
    def __init__(self, 
                 value, 
                 E_units='keV', 
                 default_units='keV',
                 name=None, 
                 check_value=None, 
                 min_value=None, 
                 max_value=None,
                 units_name=None,
                 is_optional=False,
                 extra_metadata=None):

        _allowed_units = ['keV', 'eV', 'MeV', 'GeV', 'TeV', 'Hz', 'MHz', 'GHz']

        super().__init__(value=value,
                         units=E_units,
                         default_units=default_units,
                         check_value=check_value,
                         name=name,
                         allowed_units=_allowed_units,
                         min_value=min_value,
                         max_value=max_value,
                         units_name=units_name,
                         is_optional=is_optional,
                         extra_metadata=extra_metadata)

class SpectralBoundary(Energy):
    def __init__(self, 
                 value, 
                 E_units='keV', 
                 default_units='keV', 
                 name=None, 
                 check_value=None, 
                 min_value=None, 
                 max_value=None, 
                 units_name=None,
                 is_optional=False,
                 extra_metadata=None):
    
        # retro-compatibility with integral plugin
        if check_value is None:
            check_value = self.check_energy_value
    
        super().__init__(value=value, 
                         E_units=E_units, 
                         default_units=default_units, 
                         name=name, 
                         check_value=check_value, 
                         min_value=min_value, 
                         max_value=max_value, 
                         units_name=units_name,
                         is_optional=is_optional,
                         extra_metadata=extra_metadata)
        
    @staticmethod
    def check_energy_value(value, units, name): 
        pass #the new-style check is anyway called before
        

class DetectionThreshold(Float):
    owl_uris = ("http://odahub.io/ontology#DetectionThreshold",)    
    def __init__(self, 
                 value, 
                 units='sigma', 
                 name=None, 
                 min_value=None, 
                 max_value=None,
                 is_optional=False, 
                 extra_metadata=None):
        _allowed_units = ['sigma']

        super().__init__(value=value,
                         units=units,
                         check_value=None,
                         name=name,
                         allowed_units=_allowed_units,
                         min_value=min_value,
                         max_value=max_value,
                         is_optional=is_optional,
                         extra_metadata=extra_metadata)

    # 'sigma' is not astropy unit, so need to override methods
    def get_value_in_units(self, units):
        return self.value
    
    def set_par_internal_value(self, value):
        if value is not None and value != '':
            self._value = self.default_type(value) 
        else:
            self._value = None

class UserCatalog(Parameter):
    def __init__(self, value=None, name_format='str', name=None, extra_metadata=None):
        _allowed_units = ['str']
        super().__init__(value=value,
                         par_format=name_format,
                         check_value=None,
                         name=name,
                         allowed_units=_allowed_units,
                         is_optional=True,
                         extra_metadata=extra_metadata)

class Boolean(Parameter):
    owl_uris = ('http://www.w3.org/2001/XMLSchema#bool',"http://odahub.io/ontology#Boolean")
    
    def __init__(self, value, name=None, is_optional=False, extra_metadata=None):

        self._true_rep = ['True', 'true', 'yes', '1', True]
        self._false_rep = ['False', 'false', 'no', '0', False]
        super().__init__(value=value,
                         name=name,
                         allowed_values=self._true_rep+self._false_rep,
                         is_optional=is_optional,
                         extra_metadata = extra_metadata
                         )

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, v):
        if v in self._false_rep:
            self._value = False
        elif v in self._true_rep:
            self._value = True
        else:
            raise RequestNotUnderstood(f'Wrong value for boolean parameter {self.name}')
        
class StructuredParameter(Parameter):
    owl_uris = ("http://odahub.io/ontology#StructuredParameter")
    
    def __init__(self, 
                 value, 
                 name=None, 
                 schema={"oneOf": [{"type": "object"}, {"type": "array"}]}, 
                 is_optional=False, 
                 extra_metadata=None):
        
        self.schema = schema
        
        if self.schema is None:
            logger.warning("Parameter %s: Schema is not defined, will allow any structure.", name)
            
        super().__init__(value=value,
                         name=name,
                         is_optional=is_optional,
                         extra_metadata=extra_metadata)
    
    def check_schema(self):
        if self.schema is not None:
            validate(self._value, self.schema)
        
    def additional_check(self):
        # should raise AssertionError if wrong
        pass
    
    def check_value(self):
        try:
            self.check_schema()
            self.additional_check()
        except (AssertionError, ValidationError):
            raise RequestNotUnderstood(f'Wrong value of structured parameter {self.name}')
        except SchemaError:
            raise RuntimeError(f"Wrong schema for parameter {self.name}: {self.schema}")
    
    def get_default_value(self):
        if self.value is None:
            return None
        return json.dumps(self.value, sort_keys=True)


with open(os.path.join(conf_dir, "phosphoros_filters.json"), 'r') as fd:
    phosphoros_filters = json.load(fd)

class PhosphorosFiltersTable(StructuredParameter):
    owl_uris = ('http://odahub.io/ontology#PhosphorosFiltersTable')
    
    def __init__(self, value, name=None, extra_metadata=None):
        
        # TODO: either list or the whole schema may be loaded from the external file, purely based on URI.
        #       If there is no additional check, this would allow to avoid even having the class.
        #       
        #       But for the time being, as agreed, we will keep the hardcoded dedicated class.
        filter_list = phosphoros_filters
                
        schema = {"type": "object",
                  "properties": {
                      "filter": {"type": "array", 
                                 "minItems": 1,
                                 "uniqueItems": True, 
                                 "items": {"enum": filter_list}},
                      "flux": {"type": "array",
                               "minItems": 1, 
                               "uniqueItems": True, 
                               "items": {"type": "string", "minLength": 1}},
                      "flux_error": {"type": "array", 
                                     "minItems": 1,
                                     "uniqueItems": True, 
                                     "items": {"type": "string", "minLength": 1}}},
                  "additionalProperties": False,
                  "required": ["filter", "flux", "flux_error"]
                  }
        
        super().__init__(value=value, name=name, schema=schema, is_optional=False, extra_metadata=extra_metadata)
        
    def additional_check(self):
        assert len(self._value['filter']) == len(self._value['flux']) == len(self._value['flux_error'])
        