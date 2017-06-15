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

# Standard library
from datetime import datetime, date, time

#dependencies
from wtforms import Form, TextAreaField, validators, FloatField,SelectField,StringField,Field,ValidationError
from wtforms.widgets import TextInput,TextArea

# Project
# relative import eg: from .mod import f



def date_validation(form,field):
    try:
        c= datetime.strptime(field.data, "%Y-%m-%dT%H:%M:%S.%f")
    except:
        raise ValidationError('string is not iso format YYYY-MM-DDThh:mm:ss.sssss')


def scw_list_validation(form,field):
    try:
        get_scw_list(field.data)

    except:
        raise ValidationError('window list not in correct format')


def get_scw_list(data):
    return data.split(',')

class WindowsList(Field):
    widget = TextArea()

    def _value(self):
        if self.data:
            d=u', '.join(self.data)
        else:
            d=u''

        return d


    def process_formdata(self, valuelist):
        if valuelist:
            self.data = [x.strip() for x in valuelist[0].split(',')]
        else:
            self.data = []


class ParamtersForm(Form):
    instrument = SelectField(u'Instrument',  choices=[('ISGRI','ISGRI'),('Jem-X','Jem-X')])
    image_type = SelectField(u'Image Type', choices=[('Real', 'Real'), ('Dummy', 'Dummy')])
    time_format  = SelectField(u'Time', choices=[('mjd','mjd'),('iso', 'iso'),('scw_list','scw_list')],)
    E1= FloatField('E min',[validators.DataRequired()])
    E2= FloatField('E max',[validators.DataRequired()])
    T1_iso = StringField('Tstart', validators=[date_validation],default='2000-01-01T00:00:00.0')
    T2_iso = StringField('Tstop', validators=[ date_validation],default='2000-01-01T00:00:00.0')
    T1_mjd = FloatField('MJD start', [],default=0.0)
    T2_mjd = FloatField('MJD stop', [],default=0.0)
    scw_list = TextAreaField('scw', [], default="035200230010.001,035200240010.001")
