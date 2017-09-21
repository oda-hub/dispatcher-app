

from __future__ import absolute_import, division, print_function

__author__ = "Andrea Tramacere"


# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


import  logging

logger = logging.getLogger(__name__)

import json
import traceback
import sys
from .parameters import *


def view_traceback():
    ex_type, ex, tb = sys.exc_info()
    print('tb =====>')
    traceback.print_tb(tb)
    print('   <=====')
    del tb


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
        src_name= Name(name_format='str', name='src_name')
        RA = Angle(value=0.,units='deg', name='RA', )
        DEC = Angle(value=0.,units='deg', name='DEC')

        sky_coords=ParameterTuple([RA,DEC],'sky_coords')

        t1 = Time(value='2001-12-11T00:00:00.0',name='T1',Time_format_name='T_format')
        t2 = Time(value='2001-12-11T00:00:00.0',name='T2',Time_format_name='T_format')

        t_range = ParameterRange(t1, t2, 'time')

        #time_group = ParameterGroup([t_range_iso, t_range_mjd], 'time_range', selected='t_range_iso')
        #time_group_selector = time_group.build_selector('time_group_selector')


        parameters_list=[src_name,sky_coords,t_range]


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
                 input_prod_value=None,
                 catalog_name=None,
                 catalog=None):

        radius = Angle(value=radius_value,units=raidus_units, name=radius_name)


        E1_keV = SpectralBoundary(value=E1_value,E_units=E1_units,name= E1_name)
        E2_keV = SpectralBoundary(value=E2_value,E_units=E2_units,name= E2_name)

        spec_window = ParameterRange(E1_keV, E2_keV, 'spec_window')

        input_prod_list= InputProdList(value=input_prod_value,_format='names_list', name=input_prod_list_name, )

        catalog=UserCatalog(value=catalog,name_format='str',name=catalog_name)

        parameters_list=[spec_window,radius,catalog,input_prod_list]

        super(InstrumentQuery, self).__init__(name,parameters_list)







#class InputProdsQuery(BaseQuery):
#    def __init__(self,name, input_prod_list_name,input_prod_list ):
#        super(InputProdsQuery, self).__init__(name,[ProdList('names_list', input_prod_list_name, value=input_prod_list)])




class ProductQuery(BaseQuery):
    def __init__(self,
                 name,
                 parameters_list=[],
                 get_products_method=None,
                 html_draw_method=None,
                 get_dummy_products_method=None,
                 process_product_method=None,
                 **kwargs):



        super(ProductQuery, self).__init__(name,parameters_list, **kwargs)
        self._get_product_method = get_products_method
        self._html_draw_method = html_draw_method
        self._get_dummy_products_method=get_dummy_products_method
        self._process_product_method=process_product_method
        self.query_prod_list=None

    def get_products(self, instrument, config=None,**kwargs):
        if self._get_product_method is not None:
            return self._get_product_method(instrument,config=config,**kwargs)
        else:
            return None

    def get_dummy_products(self,instrument, config=None,**kwargs):
        if self._get_dummy_products_method is not None:
            return self._get_dummy_products_method(instrument,config,**kwargs)
        else:
            return None


    def get_parameters_list_as_json(self):
        l=[ {'query_name':self.name},{'product_name':self.name}]

        for par in self._parameters_list:
            l.append(par.reprJSON())


        return json.dumps(l)



    def process_product(self,instrument,query_prod_list, config=None,**kwargs):
        product_dictionary={}
        if self._process_product_method is not None and query_prod_list is not None:
            product_dictionary= self._process_product_method(instrument,query_prod_list,**kwargs)
        return product_dictionary

    def finalize_query(self,product_dictionary,data_server_query_status,prod_process_status):

        error_message=''
        status=0
        if data_server_query_status!=0:
            error_message+='error: data_server_query failed,'
            status+=1
        if prod_process_status!=0:
            error_message+='error: prod_process_query failed,'
            status+=1

        product_dictionary['error_message']=error_message
        product_dictionary['status']=status

        return product_dictionary


    def get_prod_by_name(self,name):
        return self.query_prod_list.get_prod_by_name(name)

    def run_query(self,instrument,scratch_dir,query_type='Real', config=None,**kwargs):

        data_server_query_status=0
        #query_prod_list=None
        try:
            if query_type != 'Dummy':
                self.query_prod_list = self.get_products(instrument,
                                                    config=config,
                                                    out_dir=scratch_dir)
            else:
                self.query_prod_list = self.get_dummy_products(instrument,
                                                          config=config,
                                                          out_dir=scratch_dir)
        except Exception as e:
            print("prod_query failed, Error:", e)
            print('!!! >>>Exception<<<', e)
            data_server_query_status=1
            view_traceback()
            logger.exception(e)
            #logger.exception(view_traceback())
            raise Exception(e)
        print ('data server query status',data_server_query_status)
        prod_process_status=0

        product_dictionary={}
        try:
            product_dictionary=self.process_product(instrument, self.query_prod_list)

        except Exception as e:

            print('!!! >>>Exception<<<', e)
            print("prod_process failed, Error:", e)
            prod_process_status=1
            view_traceback()
            logger.exception(e)
            #logger.exception(view_traceback())
            raise Exception(e)
        print('prod_process_status', prod_process_status)

        return self.finalize_query(product_dictionary,data_server_query_status,prod_process_status)



class ImageQuery(ProductQuery):
    def __init__(self,name,parameters_list,**kwargs):
        detection_th = DetectionThreshold(value=0.0,units='sigma', name='detection_threshold')
        if parameters_list != [] and parameters_list is not None:
            parameters_list.extend(detection_th)
        else:
            parameters_list = [detection_th]
        super(ImageQuery, self).__init__(name, parameters_list, **kwargs)


class LightCurveQuery(ProductQuery):
    def __init__(self,name,parameters_list, **kwargs):

        time_bin=TimeDelta(value=1000., name='time_bin', delta_T_format_name='time_bin_format')
        if parameters_list != [] and parameters_list is not None:
            parameters_list.extend(time_bin)
        else:
            parameters_list = [time_bin]
        super(LightCurveQuery, self).__init__(name, parameters_list, **kwargs)


class SpectrumQuery(ProductQuery):
    def __init__(self, name,parameters_list, **kwargs):
        super(SpectrumQuery, self).__init__(name, parameters_list, **kwargs)




