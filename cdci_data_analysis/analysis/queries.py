

from __future__ import absolute_import, division, print_function


from builtins import (str, super, object)


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
from .parameters import *
from .products import SpectralFitProduct,QueryOutput,QueryProductList,ImageProduct
from .io_helper import FilePath


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

    def get_logger(self):
        logger = logging.getLogger(__name__)
        return logger

    def set_par_value(self,name,value):
        p=self.get_par_by_name(name)
        #print('get par',p.name,'set value',value)
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
        s='%stype: par | name: %s |  value: %s| units: %s| units name:%s '%(' '*indent,par.name,par.value,par.units,par.units_name)
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



    def get_parameters_list_as_json(self,prod_dict=None):
        l=[ {'query_name':self.name}]

        for par in self._parameters_list:
            l.append(par.reprJSON())


        return json.dumps(l)



class SourceQuery(BaseQuery):
    def __init__(self,name):
        src_name= Name(name_format='str', name='src_name',value='test')
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
                 extra_parameters_list=[],
                 input_prod_list_name=None,
                 input_prod_value=None,
                 catalog_name=None,
                 catalog=None):



        input_prod_list= InputProdList(value=input_prod_value,_format='names_list', name=input_prod_list_name, )

        catalog=UserCatalog(value=catalog,name_format='str',name=catalog_name)

        selected_catalog = UserCatalog(value=None, name_format='str', name='selected_catalog')

        self.input_prod_list_name = input_prod_list_name
        self.catalog_name = catalog_name

        parameters_list=[catalog,input_prod_list,selected_catalog]

        if extra_parameters_list is not None and extra_parameters_list!=[]:
            parameters_list.extend(extra_parameters_list)


        super(InstrumentQuery, self).__init__(name,parameters_list)





class ProductQuery(BaseQuery):
    def __init__(self,
                 name,
                 parameters_list=[],
                 #get_products_method=None,
                 #html_draw_method=None,
                 #get_dummy_products_method=None,
                 #process_product_method=None,
                 **kwargs):



        super(ProductQuery, self).__init__(name,parameters_list, **kwargs)

        self.job=None
        self.query_prod_list=[]

    def get_products(self, instrument,run_asynch, job=None,config=None,logger=None,**kwargs):
        raise RuntimeError('needs to be implemented in derived class')


    def get_dummy_products(self,instrument, config=None,**kwargs):
        raise RuntimeError('needs to be implemented in derived class')


    def get_data_server_query(self,instrument,config=None,**kwargs):
        raise RuntimeError('needs to be implemented in derived class')


    def get_parameters_list_as_json(self,prod_dict=None):

        l=[ {'query_name':self.name}]
        prod_name=None
        if prod_dict is not None:
            for k,v in prod_dict.items():
                if v==self.name:
                    prod_name=k
        if prod_name is not None:
            l.append({'product_name':prod_name})
        else:
            l.append({'product_name': self.name})


        for par in self._parameters_list:
            l.append(par.reprJSON())

        print(l)
        return json.dumps(l)






    def get_prod_by_name(self,name):
        return self.query_prod_list.get_prod_by_name(name)



    def test_communication(self,instrument,query_type='Real',logger=None,config=None,sentry_client=None):
        if logger is None:
            logger = self.get_logger()

        query_out = QueryOutput()




        #status = 0
        message=''
        debug_message=''

        msg_str = '--> start dataserver communication test'

        print(msg_str)
        logger.info(msg_str)
        try:

            if query_type != 'Dummy':
                test_comm_query_out = instrument.test_communication(config,logger=logger)
                status=test_comm_query_out.get_status()
            else:
                status=0

            query_out.set_done(message=message, debug_message=str(debug_message),status=status)

        except Exception as e:
            if hasattr(e, 'message'):
                e_message = e.message
            else:
                e_message = 'communication error'

            if hasattr(e, 'debug_message'):
                debug_message = e.debug_message
                #debug_message = str(e)
            else:
                debug_message = ''

            query_out.set_failed('dataserver communication ',
                                 extra_message='communication error',
                                 logger=logger,
                                 sentry_client=sentry_client,
                                 excep=e,
                                 e_message=e_message,
                                 debug_message=debug_message)




        msg_str = '--> data server communication status %d\n' %query_out.get_status()
        msg_str += '--> end dataserver communication test'
        logger.info(msg_str)




        return query_out


    def test_has_products(self,instrument,query_type='Real',logger=None,config=None,scratch_dir=None,sentry_client=None):
        if logger is None:
            logger = self.get_logger()

        query_out = QueryOutput()

        #status = 0
        message = ''
        debug_message = ''
        msg_str = '--> start test has products'

        print(msg_str)
        logger.info(msg_str)

        prod_dictionary = {}
        input_prod_list=[]


        try:

            if query_type != 'Dummy':
                test_has_input_products_query_out,input_prod_list = instrument.test_has_input_products(config,instrument,logger=logger)
                status = test_has_input_products_query_out.get_status()

            else:
                status=0

            if status==0:
               query_out.set_products(['input_prod_list', 'len_prod_list'], [input_prod_list, len(input_prod_list)])
               # DONE
               query_out.set_done(message=message, debug_message=str(debug_message), status=status)

            else:
                #FAILED
                query_out.set_failed('test has input products ', extra_message='no input products found', logger=logger,
                                     sentry_client=sentry_client)

        except Exception as e:
            if hasattr(e,'message'):
                e_message=e.message
            else:
                e_message='no input products found'


            if hasattr(e,'debug_message'):
                debug_message=e.debug_message
            else:
                debug_message=''

            input_prod_list=[]
            query_out.set_products(['input_prod_list', 'len_prod_list'], [input_prod_list, len(input_prod_list)])
            query_out.set_failed( 'test has input products ',
                                  extra_message='no input products found',
                                  logger=logger,
                                  sentry_client=sentry_client,
                                  excep=e,
                                  e_message=e_message,
                                  debug_message=debug_message)


        msg_str = '--> test has products status %d\n' % query_out.get_status()
        msg_str += '--> end test has products test'
        logger.info(msg_str)


        #print("-->input_prod_list",input_prod_list)


        return query_out

    def get_query_products(self,instrument,job,run_asynch,query_type='Real',logger=None,config=None,scratch_dir=None,sentry_client=None,api=False):
        if logger is None:
            logger = self.get_logger()

        query_out = QueryOutput()
        #status=0
        message=''
        debug_message=''
        msg_str = '--> start get product query',query_type
        print(msg_str)
        logger.info(msg_str)
        backend_comment=''
        backend_warning=''
        try:
            if query_type != 'Dummy':
                q=self.get_data_server_query(instrument,config)

                res, data_server_query_out = q.run_query(call_back_url=job.get_call_back_url(), run_asynch=run_asynch, logger=logger)

                if 'comment' in data_server_query_out.status_dictionary.keys():
                    backend_comment=data_server_query_out.status_dictionary['comment']

                if 'warning' in data_server_query_out.status_dictionary.keys():
                    backend_warning=data_server_query_out.status_dictionary['warning']


                status = data_server_query_out.get_status()
                job_status = data_server_query_out.get_job_status()

                if job_status=='done':
                    job.set_done()
                else:
                    job.set_submitted()

                if job.status != 'done':
                    prod_list = QueryProductList(prod_list=[], job=job)
                else:
                    prod_list = self.build_product_list(instrument,res, scratch_dir,api=api)

                self.query_prod_list=QueryProductList(prod_list=prod_list,job=job)

            else:
                status=0
                self.query_prod_list = self.get_dummy_products(instrument,config=config,out_dir=scratch_dir,api=api)

                #self.query_prod_list = QueryProductList(prod_list=prod_list)

                job.set_done()
            #DONE


            query_out.set_done(message=message, debug_message=str(debug_message),job_status=job.status,status=status,comment=backend_comment,warning=backend_warning)
            #print('-->', query_out.status_dictionary)
        except Exception as e:
            #status=1
            job.set_failed()

            #FAILED
            if hasattr(e,'message'):
                e_message=e.message
            else:
                e_message=''


            if hasattr(e,'debug_message'):
                debug_message=e.debug_message
            else:
                debug_message=''

            query_out.set_failed('get dataserver products ',
                                 logger=logger,
                                 sentry_client=sentry_client,
                                 excep=e,
                                 e_message=e_message,
                                 debug_message=debug_message)


        msg_str = '--> data_server_query_status %d\n' % query_out.get_status()
        msg_str += '--> end product query '

        logger.info(msg_str)





        return query_out


    def process_product(self,instrument,query_prod_list, config=None,api=False,**kwargs):
        query_out = QueryOutput()
        if self.process_product_method is not None and query_prod_list is not None:
            query_out= self.process_product_method(instrument,query_prod_list,api=api,**kwargs)
        return query_out

    def process_query_product(self,
                              instrument,
                              job,
                              query_type='Real',
                              logger=None,
                              config=None,
                              sentry_client=None,
                              api=False,
                              backend_warning='',
                              backend_comment='',
                              **kwargs):
        if logger is None:
            logger = self.get_logger()


        #status = 0
        message = ''
        debug_message = ''

        msg_str = '--> start prodcut processing'
        print(msg_str)
        logger.info(msg_str)

        process_products_query_out = QueryOutput()

        try:
            process_products_query_out=self.process_product(instrument,self.query_prod_list,api=api,**kwargs)

            process_products_query_out.prod_dictionary['session_id'] = job.session_id
            process_products_query_out.prod_dictionary['job_id'] = job.job_id

            status = process_products_query_out.get_status()

            job.set_done()
            #DONE
            process_products_query_out.set_done( message=message, debug_message=str(debug_message), job_status=job.status,status=status,comment=backend_comment,warning=backend_warning)

        except Exception as e:
            #status=1
            job.set_failed()
            # FAILED
            process_products_query_out.set_failed('product processing',
                                                  extra_message='product processing failed',
                                                  logger=logger,
                                                  sentry_client=sentry_client,
                                                  excep=e)


        msg_str = '==>prod_process_status %d\n' % process_products_query_out.get_status()
        msg_str += '--> end product process'
        logger.info(msg_str)


        return process_products_query_out




    def run_query(self,instrument,scratch_dir,job,run_asynch,query_type='Real', config=None,logger=None,sentry_client=None,api=False):

        print ('--> running query for ',instrument.name,'with config',config)
        if logger is None:
            logger = self.get_logger()

        query_out = self.test_communication(instrument,query_type=query_type,logger=logger,config=config,sentry_client=sentry_client)

        input_prod_list=None
        if query_out.status_dictionary['status'] == 0:
            query_out=self.test_has_products(instrument,query_type=query_type, logger=logger, config=config,scratch_dir=scratch_dir,sentry_client=sentry_client)
            input_prod_list=query_out.prod_dictionary['input_prod_list']




        if query_out.status_dictionary['status'] == 0:
            query_out = self.get_query_products(instrument,job,run_asynch, query_type=query_type, logger=logger, config=config,scratch_dir=scratch_dir,sentry_client=sentry_client,api=api)




        if query_out.status_dictionary['status'] == 0:
            if job.status!='done':

                query_out.prod_dictionary = {}
                # TODO: add check if is asynch
                # TODO: the asynch status will be in the qery_out class
                # TODO: if asynch and running return proper query_out
                # TODO: if asynch and done proceed

            else:
                if query_out.status_dictionary['status'] == 0:
                    #print('-->',query_out.status_dictionary)
                    if 'comment' in query_out.status_dictionary.keys():
                        backend_comment = query_out.status_dictionary['comment']
                    else:
                        backend_comment=''
                    if 'warning' in query_out.status_dictionary.keys():
                        backend_warning = query_out.status_dictionary['warning']
                    else:
                        backend_warning=''
                    query_out = self.process_query_product(instrument,
                                                           job,
                                                           logger=logger,
                                                           config=config,
                                                           sentry_client=sentry_client,api=api,
                                                           backend_comment=backend_comment,
                                                           backend_warning=backend_warning)

                    #print('-->', query_out.status_dictionary)
            #attach this at the end, anyhow
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

        self.query_prod_list = None


    def check_file_exist(self,files_list,out_dir=None):
        if files_list==[''] or files_list==None:

            raise RuntimeError('file list empty')

        for f in   files_list:
            #print('f',f,type(f))
            if f is not None:
                file_path = FilePath(file_name=f,file_dir=out_dir)
                #print(f,out_dir)
                if file_path.exists()==True:
                    pass
                else:
                    raise  RuntimeError('file %s does not exist in dir %s '%(f,out_dir))


    def process_product(self,instrument,job, config=None,out_dir=None,**kwargs):
        raise RuntimeError('this method has to be implemented in the derived class')


    def process_query_product(self,instrument,job,query_type='Real',logger=None,config=None,scratch_dir=None,sentry_client=None,api=False,**kwargs):
        if logger is None:
            logger = self.get_logger()

        #status = 0
        message = ''
        debug_message = ''

        msg_str = '--> start prodcut processing'
        print(msg_str)
        #print ('kwargs',kwargs)
        logger.info(msg_str)

        process_product_query_out = QueryOutput()

        try:
            process_product_query_out=self.process_product(instrument,job,out_dir=scratch_dir,**kwargs)
            status = process_product_query_out.get_status()
            #DONE
            process_product_query_out.set_done(message=message, debug_message=str(debug_message),status=status)
        except Exception as e:
            #FAILED
            process_product_query_out.set_failed('product post processing',
                                 extra_message='product post processing failed',
                                 logger=logger,
                                 sentry_client=sentry_client,
                                 excep=e)



        msg_str = '==>prod_process_status %d\n' % process_product_query_out.get_status()
        msg_str += '--> end product process'
        logger.info(msg_str)


        return process_product_query_out



    def run_query(self,instrument,scratch_dir,job,run_asynch,query_type='Real', config=None,logger=None,sentry_client=None,api=False):

        if logger is None:
            logger = self.get_logger()

        query_out = self.process_query_product(instrument,job,logger=logger, config=config,scratch_dir=scratch_dir,sentry_client=sentry_client,api=api)
        if query_out.status_dictionary['status'] == 0:
            job.set_done()
        else:
            job.set_failed()

        return query_out



class ImageQuery(ProductQuery):
    def __init__(self,name,parameters_list=[],**kwargs):
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
    def __init__(self,name,parameters_list=[], **kwargs):

        time_bin=TimeDelta(value=1000., name='time_bin', delta_T_format_name='time_bin_format')
        if parameters_list != [] and parameters_list is not None:
            parameters_list.append(time_bin)
        else:
            parameters_list = [time_bin]
        super(LightCurveQuery, self).__init__(name, parameters_list, **kwargs)


class SpectrumQuery(ProductQuery):
    def __init__(self, name,parameters_list=[], **kwargs):

        #xspec_model =Name(name_format='str', name='xspec_model',value='powerlaw')
        #if parameters_list != [] and parameters_list is not None:
        #    parameters_list.append(xspec_model)
        #else:
        #    parameters_list = [xspec_model]


        super(SpectrumQuery, self).__init__(name, parameters_list, **kwargs)


class InputDataQuery(ProductQuery):
    def __init__(self, name,parameters_list=[], **kwargs):

        #xspec_model =Name(name_format='str', name='xspec_model',value='powerlaw')
        #if parameters_list != [] and parameters_list is not None:
        #    parameters_list.append(xspec_model)
        #else:
        #    parameters_list = [xspec_model]


        super(InputDataQuery, self).__init__(name, parameters_list, **kwargs)

class SpectralFitQuery(PostProcessProductQuery):
    def __init__(self, name,parameters_list=[], **kwargs):

        xspec_model =Name(name_format='str', name='xspec_model',value='powerlaw')


        ph_file = Name(name_format='str', name='ph_file_name', value='')
        rmf_file = Name(name_format='str', name='rmf_file_name', value='')
        arf_file = Name(name_format='str', name='arf_file_name', value='')

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


    def process_product(self,instrument,job,out_dir=None,api=False):

        _c_list=[]
        src_name = instrument.get_par_by_name('src_name').value

        ph_file=instrument.get_par_by_name('ph_file_name').value
        rmf_file=instrument.get_par_by_name('rmf_file_name').value
        arf_file=instrument.get_par_by_name('arf_file_name').value

        e_min_kev=np.float(instrument.get_par_by_name('E1_keV').value)
        e_max_kev=np.float(instrument.get_par_by_name('E2_keV').value)


        if instrument.name=='isgri':
            e_min_kev=30.
            e_max_kev=300.

        if 'jemx' in instrument.name:
            e_min_kev=5.
            e_max_kev=20.

        for f in [ph_file,rmf_file,arf_file]:
            if    f is not None and f!='None':
                _c_list.append(f)

        #print('e_min_kev',e_min_kev)
        #print('e_max_kev', e_max_kev)

        self.check_file_exist(_c_list,out_dir=out_dir)

        query_out = QueryOutput()
        try:
            query_out.prod_dictionary['image'] = SpectralFitProduct('spectral_fit',ph_file,arf_file,rmf_file,file_dir=out_dir).run_fit(e_min_kev=e_min_kev,
                                                                                                                                       e_max_kev=e_max_kev,
                                                                                                                                       xspec_model=instrument.get_par_by_name('xspec_model').value,)

        except Exception as e:

            raise RuntimeError('spectral fit failed, Xspec Error: %s'%e)


        query_out.prod_dictionary['job_id'] = job.job_id
        query_out.prod_dictionary['spectrum_name'] = src_name

        query_out.prod_dictionary['ph_file_name'] = ph_file
        query_out.prod_dictionary['arf_file_name'] = arf_file
        query_out.prod_dictionary['rmf_file_name'] = rmf_file

        query_out.prod_dictionary['session_id'] = job.session_id
        query_out.prod_dictionary['download_file_name'] = 'spectra.tar.gz'
        query_out.prod_dictionary['prod_process_maessage'] = ''

        return query_out



# class ImageProcessQuery(PostProcessProductQuery):
#
#     def __init__(self, name, parameters_list=[], **kwargs):
#         image_file = Name(name_format='str', name='image_file_name', value='')
#         image_scale_min = Float( name='image_scale_min', value=None)
#         image_scale_max = Float( name='image_scale_max', value=None)
#         download_files = Float(name='str', value=None)
#
#         p_list = [image_file, image_scale_min, image_scale_max]
#         if parameters_list != [] and parameters_list is not None:
#             parameters_list.extend(p_list)
#         else:
#             parameters_list = p_list[::]
#
#         super(ImageProcessQuery, self).__init__(name,
#                                                parameters_list,
#                                                # get_products_method=None,
#                                                # get_dummy_products_method=None,
#                                                **kwargs)
#
#     def process_product(self, instrument, job, out_dir=None):
#
#         src_name = instrument.get_par_by_name('src_name').value
#
#         image_file = instrument.get_par_by_name('image_file_name').value
#         download_files = instrument.get_par_by_name('image_download_files').value
#         image_scale_min = instrument.get_par_by_name('image_scale_min').value
#         image_scale_max = instrument.get_par_by_name('image_scale_max').value
#         catalog=instrument.get_par_by_name('image_catalog').value
#         self.check_file_exist([image_file], out_dir=out_dir)
#
#         query_out = QueryOutput()
#         try:
#             query_out.prod_dictionary['image'] = ImageProduct.from_fits_file(image_file).get_html_draw(vmin=image_scale_min,
#                                                 vmax=image_scale_max,
#                                                 catalog=catalog)
#
#         except Exception as e:
#
#             raise RuntimeError('image update failed with error: %s' % e)
#
#         query_out.prod_dictionary['job_id'] = job.job_id
#         query_out.prod_dictionary['session_id'] = job.session_id
#         query_out.prod_dictionary['spectrum_name'] = src_name
#         query_out.prod_dictionary['catalog'] =catalog.get_dictionary()
#         query_out.prod_dictionary['file_name'] = [download_files]
#         query_out.prod_dictionary['download_file_name'] = 'image.tgz'
#         query_out.prod_dictionary['prod_process_message'] = ''
#
#         return query_out
