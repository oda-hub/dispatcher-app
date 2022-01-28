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

# TODO importing six necessary for compatibility with the plugin, to be removed in the future with the necessary adaptations
import six
import decorator
import logging

from astropy.time import Time as astropyTime
from astropy.time import TimeDelta as astropyTimeDelta

from astropy.coordinates import Angle as astropyAngle

import numpy as np

from typing import Union

logger = logging.getLogger(__name__)


@decorator.decorator
def check_par_list(func, par_list, *args, **kwargs):
    for par in par_list:
        if isinstance(par, Parameter):
            pass
        else:
            raise RuntimeError('each parameter in the par_list has to be an instance of Parameters')

        return func(par_list, *args, **kwargs)


# TODO this class seems not to be in use anywhere, not even the plugins
class ParameterGroup(object):

    def __init__(self, par_list, name, exclusive=True, def_selected=None, selected=None):
        self.name = name
        self._par_list = par_list
        self._check_pars(par_list)
        self.exclusive = True

        self.msk = np.ones(len(par_list), dtype=np.bool)

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


class ParameterTuple(object):

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


class Parameter(object):
    def __init__(self,
                 value=None,
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

                 ):

        if (units is None or units == '') and \
                default_units is not None and default_units != '':
            # TODO ideally those should be renamed as singular (unit and default_unit)
            #  but they are as such because they're used also in plugins
            #
            units = default_units

        if (par_format is None or par_format == '') and \
                par_default_format is not None and par_default_format != '':
            par_format = par_default_format

        self.check_value = check_value

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

        self._allowed_units = allowed_units
        self._allowed_values = allowed_values
        self._allowed_types = allowed_types
        self.name = name
        self.units = units
        self.default_units = default_units
        self.units_name = units_name
        self.default_type = default_type
        self.par_format=par_format
        self.par_default_format=par_default_format
        self.par_format_name=par_format_name
        self.value = value

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
            if self.check_value is not None:
                self.check_value(v, units=self.units, name=self.name, par_format=self.par_format)
            if self._allowed_values is not None:
                if v not in self._allowed_values:
                    raise RuntimeError(f'value {v} not allowed, allowed= {self._allowed_values}')
            if isinstance(v, str):
                self._value = v.strip()
            else:
                self._value = v
        else:
            self._value = None

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

    def get_value_in_format(self, units):
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
    def check_value(val, units=None, name=None, par_format=None):
        pass

    def reprJSON(self):
        return dict(name=self.name, units=self.units, value=self.value)


class Name(Parameter):
    def __init__(self, value=None, name_format='str', name=None):
        _allowed_units = ['str']
        super().__init__(value=value,
                         units=name_format,
                         check_value=self.check_name_value,
                         name=name,
                         allowed_units=_allowed_units)

    @staticmethod
    def check_name_value(value, units=None, name=None, par_format=None):
        pass


class Float(Parameter):
    def __init__(self, value=None, units=None, name=None, allowed_units=None, default_units=None, check_value=None):

        if check_value is None:
            check_value = self.check_float_value

        super().__init__(value=value,
                         units=units,
                         check_value=check_value,
                         default_units=default_units,
                         name=name,
                         default_type=float,
                         allowed_units=allowed_units)

    @property
    def value(self):
        return self._v

    @value.setter
    def value(self, v):
        if v is not None and v != '':
            self.check_value(v, name=self.name, units=self.units)
            self._v = float(v)
        else:
            self._v = None

    def get_value_in_units(self, units):
        logger.warning(f'no explict conversion implemented for the parameter {self.name}, '
                       f'the non converted value is returned')
        return self.value

    def get_value_in_default_units(self):
        self.check_value(self.value, name=self.name, units=self.units)
        return float(self.value) if self.value is not None else None

    def get_default_value(self):
        return self.get_value_in_default_units()

    @staticmethod
    def check_float_value(value, units=None, name=None):
        if value is None or value == '':
            pass
        else:
            try:
                float(value)
            except:
                raise RuntimeError(f'the Float parameter {name} cannot be assigned the value {value} '
                                   f'of type {type(value).__name__}')


class Integer(Parameter):
    def __init__(self, value=None, units=None, name=None, check_value=None):

        _allowed_units = None

        if check_value is None:
            check_value = self.check_int_value

        super().__init__(value=value,
                         units=units,
                         check_value=check_value,
                         default_type=int,
                         allowed_types=[int],
                         name=name,
                         allowed_units=_allowed_units)

    @property
    def value(self):
        return self._v

    @value.setter
    def value(self, v):
        if v is not None and v != '':
            self.check_value(v, name=self.name, units=self.units)
            self._v = int(v)
        else:
            self._v = None

    def get_value_in_default_units(self):
        self.check_value(self.value, name=self.name, units=self.units)
        return int(self.value) if self.value is not None else None

    @staticmethod
    def check_int_value(value, units=None, name=None):
        # print('check type of ',name,'value', value, 'type',type(value))
        if value is None or value == '':
            pass
        else:
            if isinstance(value, float):
                message = f'{value} is an invalid value for {name} since it cannot be used as an Integer'
                logger.error(message)
                raise RuntimeError(message)
            try:
                int(value)
            except:
                raise RuntimeError(f'the Integer parameter {name} cannot be assigned the value {value} '
                                   f'of type {type(value).__name__}')


class Time(Parameter):
    def __init__(self, value=None, T_format='isot', name=None, Time_format_name=None, par_default_format='isot'):

        super().__init__(value=value,
                         par_format=T_format,
                         par_format_name=Time_format_name,
                         par_default_format=par_default_format,
                         name=name)

    def get_default_value(self):
        return self.get_value_in_default_format()

    def get_value_in_format(self, par_format):
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
        par_format = self.par_format
        self._set_time(v, par_format=par_format)

    def _set_time(self, value, par_format):
        self._astropy_time = astropyTime(value, format=par_format)
        self._value = value


class TimeDelta(Time):
    def __init__(self, value=None, delta_T_format='sec', name=None, delta_T_format_name=None, par_default_format='sec'):

        super().__init__(value=value,
                         T_format=delta_T_format,
                         Time_format_name=delta_T_format_name,
                         par_default_format=par_default_format,
                         name=name)

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
        self._astropy_time_delta = astropyTimeDelta(value, format=format)
        self._value = value


class InputProdList(Parameter):
    # TODO removal of the leading underscore would probably make sense
    def __init__(self, value=None, _format='names_list', name: str = None):
        _allowed_units = ['names_list']

        if value is None:
            value = []

        super().__init__(value=value,
                         par_format=_format,
                         check_value=self.check_list_value,
                         name=name,
                         allowed_units=_allowed_units)

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
            if self.check_value is not None:
                self.check_value(v, list_format=self.par_format, name=self.name)
            if self._allowed_values is not None:
                if v not in self._allowed_values:
                    raise RuntimeError(f'value {v} not allowed, allowed= {self._allowed_values}')
            if v == [''] or v is None or str(v) == '':
                self._value = ['']
            else:
                self._value = v
        else:
            self._value = ['']
        self._value = self._split(self._value)
        # print ('set to ',self._value)

    @staticmethod
    def check_list_value(value, list_format, name='par'):
        if list_format == 'names_list':
            # TODO the condition 'isinstance(str(value), str))' was quite unclear to me, and probably useless since could lead to unexpected behavior
            if not isinstance(value, (list, str, float, int)):
                raise RuntimeError(f'value of the parameter {name} is not a valid product list format, but {type(value).__name__} has been found')
        else:
            raise RuntimeError(f'{name} units not valid {list_format}')


class Angle(Float):
    def __init__(self, value=None, units=None, default_units='deg', name=None):

        super().__init__(value=value,
                         units=units,
                         # TODO can we safely make this assumption?
                         default_units=default_units,
                         name=name,
                         allowed_units=None)

    def get_value_in_default_units(self):
        return self.get_value_in_units(self.default_units)

    def get_value_in_units(self, units) -> Union[str, float, None]:
        return getattr(self._astropy_angle, units)

    @property
    def value(self):
        return self._astropy_angle.value

    @value.setter
    def value(self, v, units=None):
        if units is None:
            units = self.units

        self._set_angle(v, units=units)

    def _set_angle(self, value, units):
        if value == '' or value is None:
            pass
        else:
            self._astropy_angle = astropyAngle(value, unit=units)
            self._value = self._astropy_angle.value


class Energy(Float):
    def __init__(self, value=None, E_units='keV', name=None, check_value=None):
        if check_value is None:
            check_value = self.check_energy_value

        _allowed_units = ['keV', 'eV', 'MeV', 'GeV', 'TeV', 'Hz', 'MHz', 'GHz']

        super().__init__(value=value,
                         units=E_units,
                         default_units='keV',
                         check_value=check_value,
                         name=name,
                         allowed_units=_allowed_units)

    # TODO re-introduced for retro-compatibility
    @staticmethod
    def check_energy_value(value, units=None, name=None):
        Float.check_float_value(value, units=units, name=name)


class SpectralBoundary(Energy):
    pass


class DetectionThreshold(Float):
    def __init__(self, value=None, units='sigma', name=None):
        _allowed_units = ['sigma']

        super().__init__(value=value,
                         units=units,
                         # TODO to check if it's correct
                         check_value=self.check_float_value,
                         name=name,
                         allowed_units=_allowed_units)


class UserCatalog(Parameter):
    def __init__(self, value=None, name_format='str', name=None):
        _allowed_units = ['str']
        super().__init__(value=value,
                         par_format=name_format,
                         check_value=self.check_name_value,
                         name=name,
                         allowed_units=_allowed_units)

    @staticmethod
    def check_name_value(value, units=None, name=None, par_format=None):
        pass
