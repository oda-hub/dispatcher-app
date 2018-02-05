

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


import json
import traceback
import sys
from .parameters import *
from .products import SpectralFitProduct,QueryOutput
from ..analysis.job_manager import Job

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
        self.input_prod_list_name=input_prod_list_name
        self.catalog_name=catalog_name
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
        self.job=None


    def get_products(self, instrument, job=None,config=None,**kwargs):
        if self._get_product_method is not None:
            return self._get_product_method(instrument,config=config,job=job,**kwargs)
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






    def get_prod_by_name(self,name):
        return self.query_prod_list.get_prod_by_name(name)


   # def test_communication(self):


    #def set_message_dictionary(self,status,error_message='',debug_message='',product_dictionary=None):
     #   if product_dictionary is None:
     #       product_dictionary={}

    #    product_dictionary['status']=status
    #    product_dictionary['error_message']=error_message
    #    product_dictionary['debug_message']=debug_message

        return product_dictionary

    def test_communication(self,instrument,query_type='Real',logger=None,config=None):
        print('logger')
        if logger is None:
            logger = logging.getLogger(__name__)
        status = 0
        message=''
        debug_message=''
        communication_status=0
        msg_str = '--> start dataserver communication test'
        print(msg_str)
        logger.info(msg_str)
        try:

            if query_type != 'Dummy':
                communication_status = instrument.test_communication(config)
        except Exception as e:
            print("dataserver communication failed, Error:", e)
            print('!!! >>>Exception<<<', e)
            status = 1
            message='dataserver communication failed'
            debug_message=e.message
            view_traceback()
            logger.exception(e)

        msg_str = '--> data server communication status %d\n' % status
        msg_str += '--> end dataserver communication test'
        logger.info(msg_str)

        query_out=QueryOutput()
        query_out.set_status(status, message, debug_message=str(debug_message))

        return query_out,communication_status

    def test_is_busy(self,instrument,communication_status,logger=None,config=None):
        print('logger')
        if logger is None:
            logger = logging.getLogger(__name__)

        status = 0
        message = ''
        debug_message = ''
        msg_str = '--> start data server is busy query'
        print(msg_str)
        logger.info(msg_str)
        data_server_busy_status = 0
        if communication_status == 'busy':

            try:
                communication_status = instrument.test_busy(config)
                status = 0
            except Exception as e:
                print("data server  bust, Error:", e)
                print('!!! >>>Exception<<<', e)
                data_server_busy_status = 1
                view_traceback()
                logger.exception(e)
                status=1
                message='dataserver is busy'
            msg_str = '-->data_server_busy_status %d\n' % data_server_busy_status
            msg_str += '--> end data server is busy query test'

            logger.info(msg_str)

        query_out = QueryOutput()
        query_out.set_status(status, message, debug_message=str(debug_message))

        return query_out,communication_status

    def test_has_products(self,instrument,query_type='Real',logger=None,config=None,scratch_dir=None):
        status = 0
        message = ''
        debug_message = ''
        msg_str = '--> start test has products'
        print(msg_str)
        logger.info(msg_str)

        prod_dictionary = {}
        input_prod_list=[]


        try:

            if query_type != 'Dummy':
                input_prod_list = instrument.test_has_input_products(config,instrument)

                if len(input_prod_list) < 1:
                    status = 1
                    message = 'no input products'



        except Exception as e:
            print("test has products failed, Error:", e)
            print('!!! >>>Exception<<<', e)
            status = 1
            message='test has products failed'
            debug_message=e.message
            view_traceback()
            logger.exception(e)

        msg_str = '--> dtest has products status %d\n' % status
        msg_str += '--> end test has products test'
        logger.info(msg_str)


        print("-->input_prod_list",input_prod_list)

        query_out = QueryOutput()

        query_out.set_products(['input_prod_list','len_prod_list'],[input_prod_list,len(input_prod_list)])
        query_out.set_status(status, message, debug_message=str(debug_message))


        return query_out

    def get_query_products(self,instrument,job,query_type='Real',logger=None,config=None,scratch_dir=None):
        # query
        status=0
        message=''
        debug_message=''
        msg_str = '--> start get prodcut query'
        print(msg_str)
        logger.info(msg_str)
        try:
            if query_type != 'Dummy':
                self.query_prod_list = self.get_products(instrument,
                                                         config=config,
                                                         out_dir=scratch_dir,
                                                         job=job)
            else:
                self.query_prod_list = self.get_dummy_products(instrument,
                                                               config=config,
                                                               out_dir=scratch_dir)
                job.set_done()

        except Exception as e:
            print("prod_query failed, Error:", e)
            print('!!! >>>Exception<<<', e)
            view_traceback()
            logger.exception(e)
            status=1
            message='dataserver get product query failed'
            debug_message=e.message

        msg_str = '--> data_server_query_status %d\n' % status
        msg_str += '--> end product query '

        logger.info(msg_str)

        query_out = QueryOutput()

        query_out.set_status(status, message, debug_message=str(debug_message))

        return query_out


    def process_product(self,instrument,query_prod_list, config=None,**kwargs):
        query_out = QueryOutput()
        if self._process_product_method is not None and query_prod_list is not None:
            query_out= self._process_product_method(instrument,query_prod_list,**kwargs)
        return query_out

    def process_query_product(self,instrument,query_type='Real',logger=None,config=None,**kwargs):
        status = 0
        message = ''
        debug_message = ''

        msg_str = '--> start prodcut processing'
        print(msg_str)
        logger.info(msg_str)

        query_out = QueryOutput()

        try:
            query_out=self.process_product(instrument, self.query_prod_list,**kwargs)

        except Exception as e:

            print('!!! >>>Exception<<<', e)
            print("prod_process failed, Error:", e)
            view_traceback()
            logger.exception(e)
            status=1
            message='product processig failed'
            debug_message = e.message

        msg_str = '==>prod_process_status %d\n' % status
        msg_str += '--> end product process'
        logger.info(msg_str)

        query_out.set_status(status, message, debug_message=str(debug_message))
        return query_out




    def run_query(self,instrument,scratch_dir,job,query_type='Real', config=None,logger=None):
        input_prod_list=None



        query_out,communication_status = self.test_communication(instrument,query_type=query_type,logger=logger,config=config)

        if query_out.status_dictionary['status']==0:
            query_out,communication_status = self.test_is_busy(instrument,communication_status,logger=logger,config=config)

        if query_out.status_dictionary['status'] == 0:
            query_out=self.test_has_products(instrument,query_type=query_type, logger=logger, config=config,scratch_dir=scratch_dir)
            input_prod_list=query_out.prod_dictionary['input_prod_list']




        if query_out.status_dictionary['status'] == 0:
            query_out = self.get_query_products(instrument,job, query_type=query_type, logger=logger, config=config,scratch_dir=scratch_dir)

        if query_out.status_dictionary['status'] == 0:
            if job.status!='done':


                query_out.prod_dictionary = {}
                # TODO: add check if is asynch
                # TODO: the asynch status will be in the qery_out class
                # TODO: if asynch and running return proper query_out
                # TODO: if asynch and done proceed

            else:
                if query_out.status_dictionary['status'] == 0:
                    query_out = self.process_query_product(instrument, logger=logger, config=config)


                if input_prod_list is not None:
                    query_out.prod_dictionary['input_prod_list']=input_prod_list

        return query_out







class PostProcessProductQuery(ProductQuery):
    def __init__(self,
                 name,
                 parameters_list=[],
                 get_products_method=None,
                 html_draw_method=None,
                 get_dummy_products_method=None,
                 process_product_method=None,
                 **kwargs):

        super(PostProcessProductQuery, self).__init__(name, parameters_list, **kwargs)
        self._get_product_method = get_products_method
        self._html_draw_method = html_draw_method
        self._get_dummy_products_method = get_dummy_products_method
        self._process_product_method = process_product_method
        self.query_prod_list = None



    def process_product(self,instrument,query_prod_list, config=None,**kwargs):
        query_out = QueryOutput()
        if self._process_product_method is not None and query_prod_list is not None:
            query_out= self._process_product_method(instrument,query_prod_list,**kwargs)
        return query_out

    def process_query_product(self,instrument,query_type='Real',logger=None,config=None,scratch_dir=None,**kwargs):
        status = 0
        message = ''
        debug_message = ''

        msg_str = '--> start prodcut processing'
        print(msg_str)
        print ('kwargs',kwargs)
        logger.info(msg_str)

        query_out = QueryOutput()

        try:
            query_out=self.process_product(instrument,out_dir=scratch_dir,**kwargs)

        except Exception as e:

            print('!!! >>>Exception<<<', e)
            print("prod_process failed, Error:", e)
            view_traceback()
            logger.exception(e)
            status=1
            message='product processig failed'
            debug_message = e.message

        msg_str = '==>prod_process_status %d\n' % status
        msg_str += '--> end product process'
        logger.info(msg_str)

        query_out.set_status(status, message, debug_message=str(debug_message))
        return query_out



    def run_query(self,instrument,scratch_dir,query_type='Real', config=None,logger=None):

        #query_out = self.get_query_products(instrument, query_type=query_type, logger=logger, config=config,scratch_dir=scratch_dir)
        #if query_out.status_dictionary['status'] == 0:

        query_out = self.process_query_product(instrument, logger=logger, config=config,scratch_dir=scratch_dir)

        return query_out

class ImageQuery(ProductQuery):
    def __init__(self,name,parameters_list,**kwargs):
        detection_th = DetectionThreshold(value=0.0,units='sigma', name='detection_threshold')
        if parameters_list != [] and parameters_list is not None:
            parameters_list.append(detection_th)
        else:
            parameters_list = [detection_th]

        image_scale_min=Float(value=None,name='image_scale_min')
        image_scale_max = Float(value=None, name='image_scale_max')
        parameters_list.extend([image_scale_min, image_scale_max])
        super(ImageQuery, self).__init__(name, parameters_list, **kwargs)


class LightCurveQuery(ProductQuery):
    def __init__(self,name,parameters_list, **kwargs):

        time_bin=TimeDelta(value=1000., name='time_bin', delta_T_format_name='time_bin_format')
        if parameters_list != [] and parameters_list is not None:
            parameters_list.append(time_bin)
        else:
            parameters_list = [time_bin]
        super(LightCurveQuery, self).__init__(name, parameters_list, **kwargs)


class SpectrumQuery(ProductQuery):
    def __init__(self, name,parameters_list, **kwargs):

        #xspec_model =Name(name_format='str', name='xspec_model',value='powerlaw')
        #if parameters_list != [] and parameters_list is not None:
        #    parameters_list.append(xspec_model)
        #else:
        #    parameters_list = [xspec_model]


        super(SpectrumQuery, self).__init__(name, parameters_list, **kwargs)


class InputDataQuery(ProductQuery):
    def __init__(self, name,parameters_list, **kwargs):

        #xspec_model =Name(name_format='str', name='xspec_model',value='powerlaw')
        #if parameters_list != [] and parameters_list is not None:
        #    parameters_list.append(xspec_model)
        #else:
        #    parameters_list = [xspec_model]


        super(InputDataQuery, self).__init__(name, parameters_list, **kwargs)

class SpectralFitQuery(PostProcessProductQuery):
    def __init__(self, name,parameters_list, **kwargs):

        xspec_model =Name(name_format='str', name='xspec_model',value='powerlaw')
        ph_file = Name(name_format='str', name='ph_file', value='')
        rmf_file = Name(name_format='str', name='rmf_file', value='')
        arf_file = Name(name_format='str', name='arf_file', value='')

        p_list=[xspec_model,ph_file,arf_file,rmf_file]
        if parameters_list != [] and parameters_list is not None:
            parameters_list.extend(p_list)
        else:
            parameters_list = p_list[::]


        super(SpectralFitQuery, self).__init__(name,
                                               parameters_list,
                                               #get_products_method=None,
                                               #get_dummy_products_method=None,
                                               **kwargs)


    def process_product(self,instrument,out_dir=None):
        print ('out dir',out_dir)
        src_name = instrument.get_par_by_name('src_name').value

        ph_file=instrument.get_par_by_name('ph_file').value
        rmf_file=instrument.get_par_by_name('rmf_file').value
        arf_file=instrument.get_par_by_name('arf_file').value

        query_out = QueryOutput()
        query_out.prod_dictionary['image'] = SpectralFitProduct('spectral_fit',ph_file,arf_file,rmf_file,file_dir=out_dir).run_fit(xspec_model=instrument.get_par_by_name('xspec_model').value)

        return query_out

