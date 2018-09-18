from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

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

from pathlib import Path

from astropy import wcs
from astropy.wcs import WCS

from astropy.io import fits as pf

import matplotlib

matplotlib.use('Agg', warn=False)

import matplotlib.pyplot as plt

import numpy as np

import mpld3
from mpld3 import plugins

from .plot_tools import  Image,ScatterPlot,GridPlot


from .parameters import *
from .io_helper import FilePath
from .io_helper import view_traceback, FitsFile
from .job_manager import Job


class QueryOutput(object):
    def __init__(self):
        self.prod_dictionary = {}
        self.status_dictionary = {}

        self._allowed_status_values_ = [0, 1]
        self._allowed_job_status_values_ = Job.get_allowed_job_status_values()

        self.set_status(0, job_status='unknown')


    def set_analysis_parameters(self,query_dict):
        self.prod_dictionary['analysis_paramters']=query_dict

    def dump_analysis_parameters(self,work_dir,query_dict):
        file_path=FilePath(file_dir=work_dir,file_name='anlaysis_par.json')
        with open(file_path.path, 'w')  as outfile:
            my_json_str = json.dumps(query_dict, encoding='utf-8')
            outfile.write(u'%s' % my_json_str)

    def set_products(self, keys, values):
        for k, v in zip(keys, values):
            self.prod_dictionary[k] = v

    def set_done(self, message='', debug_message='', job_status=None, status=0):
        self.set_status(status, message=message, debug_message=debug_message, job_status=job_status)

    def set_failed(self, failed_operation,
                   message_prepend_str='',
                   extra_message=None,
                   message=None,
                   logger_prepend_str='==>',
                   logger=None,
                   excep=None,
                   status=1,
                   sentry_client=None,
                   job_status=None,
                   e_message=None,
                   debug_message=''):

        self.set_query_exception(excep,
                                 failed_operation,
                                 message_prepend_str=message_prepend_str,
                                 message=message,
                                 extra_message=extra_message,
                                 logger_prepend_str=logger_prepend_str,
                                 logger=logger,
                                 status=status,
                                 sentry_client=sentry_client,
                                 job_status=job_status,
                                 e_message=e_message,
                                 debug_message=debug_message)

    # def set_progress(self):
    #    pass

    def _set_job_status(self, job_status):
        if job_status is not None:
            if job_status in self._allowed_job_status_values_:

                self.status_dictionary['job_status'] = job_status
            else:
                raise RuntimeError('job_status', job_status, ' in QueryOutput is not allowed',
                                   self._allowed_job_status_values_)

    def set_status(self, status, message='', error_message='', debug_message='', job_status=None):

        self._set_job_status(job_status)

        if status in self._allowed_status_values_:
            self.status_dictionary['status'] = status
        else:
            raise RuntimeError('status', status, ' in QueryOutput is not allowed',
                               self._allowed_status_values_)

        self.status_dictionary['message'] = str(message)
        self.status_dictionary['error_message'] = str(error_message)
        self.status_dictionary['debug_message'] = str(debug_message)

    def get_status(self):
        return self.status_dictionary['status']

    def get_job_status(self):
        return self.status_dictionary['job_status']

    def set_query_exception(self, excep,
                            failed_operation,
                            message_prepend_str='',
                            extra_message=None,
                            message=None,
                            logger_prepend_str='==>',
                            logger=None,
                            status=1,
                            sentry_client=None,
                            job_status=None,
                            e_message=None,
                            debug_message=''):

        self._set_job_status(job_status)

        if e_message is None:
            e_message = ''
            if excep is not None:
                if excep.__repr__ is None:
                    e_message = ''
                else:
                    try:
                        e_message = excep.__repr__()
                    except:

                        e_message = ''
        else:
            print('e_message', e_message)

        if sentry_client is not None:
            sentry_client.capture('raven.events.Message', message=e_message)

        print('!!! >>>Exception<<<', e_message)
        print('!!! >>>debug message<<<', debug_message)
        print('!!! failed operation', failed_operation)

        view_traceback()

        if logger is not None:
            logger.exception(e_message)
            logger.exception(debug_message)

        if message is None:
            message = '%s' % message_prepend_str
            message += 'failed: %s' % (failed_operation)
            if extra_message is not None:
                message += 'message: %s' % (extra_message)
        else:
            pass

        msg_str = '%s' % logger_prepend_str
        msg_str += 'failed: %s' % failed_operation
        msg_str += ' error: %s' % e_message
        msg_str += ' debug : %s' % debug_message
        if extra_message is not None:
            msg_str += ' message: %s' % (extra_message)

        if logger is not None:
            logger.info(msg_str)

        self.set_status(status, message=message, error_message=e_message, debug_message=str(debug_message))


class QueryProductList(object):

    def __init__(self, prod_list, job=None):
        self._prod_list = prod_list
        self.job = job

    @property
    def prod_list(self):
        return self._prod_list

    def get_prod_by_name(self, name):
        prod = None
        for prod1 in self._prod_list:
            if hasattr(prod1, 'name'):
                if prod1.name == name:
                    prod = prod1
        if prod is None:
            raise Warning('product', name, 'not found')
        return prod



class ProductData(object):

    def __init__(self,data,kw_dict=None):
        self. data=data
        self.kw_dict=kw_dict





class BaseQueryProduct(object):

    def __init__(self, name,
                 file_name=None,
                 file_dir='./',
                 name_prefix=None,
                 product_data=None):

        self.name = name
        if file_name is not None:
            print('set file phat')
            print('workig dir', file_dir)
            print('file name', file_name)
            print('name_prefix', name_prefix)
            self.file_path = FilePath(file_name=file_name, file_dir=file_dir, name_prefix=name_prefix)
            print('file_path set to', self.file_path.path)

        if product_data is not None:
            if isinstance(product_data,ProductData):
                self.data = product_data
            else:
                raise RuntimeError('data is not of the expected type',type(ProductData))


    def write(self):
        pass

    def read(self):
        pass


    def set_data(self):
        self.data_table=None
        pass

    def set_header(self):
        self._header_dict=None
        pass

    def jsonify(self):
        pass



class ImageProduct(BaseQueryProduct):
    def __init__(self, name, data, header, file_name='image.fits', **kwargs):
        self.name = name
        self.data = data
        self.header = header
        self.file_name = file_name
        super(ImageProduct, self).__init__(name, file_name=file_name, **kwargs)

    @classmethod
    def from_fits_file(cls, in_file, out_file_name, prod_name, ext=0, **kwargs):
        # hdu = pf.open(in_file)[ext]
        # print('ciccio in_file', in_file)
        hdu = FitsFile(in_file).open()[ext]

        data = hdu.data
        header = hdu.header

        return cls(name=prod_name, data=data, header=header, file_name=out_file_name, **kwargs)

    def write(self, file_name=None, overwrite=True, file_dir=None):

        # TODO: this should be file_path = self.file_path.path-> DONE AND PASSED
        file_path = self.file_path.path
        # file_path=self.file_path.get_file_path(file_name=file_name,file_dir=file_dir)
        pf.writeto(file_path, data=self.data, header=self.header, overwrite=overwrite)

    def get_html_draw(self, catalog=None, plot=False, vmin=None, vmax=None):
        print ('BUILD IMAGE CLASS')
        im=Image(data=self.data,header=self.header)
        w=600
        (r,c)=self.data.shape
        ratio=float(r)/c
        html_dict=im.get_html_draw(w=w,h=int(w*ratio),catalog=catalog)
        print('BUILD IMAGE CLASS DONE')

        res_dict = {}
        res_dict['image']=html_dict
        #res_dict['image'] = mpld3.fig_to_dict(fig)
        #res_dict['image'] =plotly.offline.plot({
        #    "data": [self.data],
        #    "layout": Layout(title="Test plotly")},output_type='div')


        res_dict['header_text'] = ''
        res_dict['table_text'] = ''
        res_dict['footer_text']=''
        #res_dict['footer_text'] = 'colorscale for normalzied significance\nmax significance=%.2f, min significance=%.2f' % (
        #vmax, vmin)

        #plt.close(fig)
        return res_dict


class LightCurveProduct(BaseQueryProduct):
    def __init__(self, name,
                 data,
                 header,
                 file_name='lc.fits',
                 src_name=None,
                 **kwargs):

        self.name = name
        self.data = data
        self.header = header
        self.file_name = file_name
        self.src_name = src_name

        super(LightCurveProduct, self).__init__(name, file_name=file_name, **kwargs)



    @classmethod
    def from_fits_file(cls, inf_file, out_file_name, prod_name, ext=0, **kwargs):
        # hdu = pf.open(inf_file)[ext]
        hdu = FitsFile(inf_file).open()[ext]
        data = hdu.data
        header = hdu.header
        return cls(name=prod_name, data=data, header=header, file_name=out_file_name, **kwargs)

    def write(self, file_name=None, overwrite=True, file_dir=None):
        # print('writing catalog file to->',)
        file_path = self.file_path.get_file_path(file_name=file_name, file_dir=file_dir)
        pf.writeto(file_path, data=self.data, header=self.header, overwrite=overwrite)





    def get_html_draw(self,x,y,dy=None,dx=None,x_label='',y_label=''):

        x = x - np.int(x.min())

        sp=ScatterPlot(w=600,h=600,x_label=x_label,y_label=y_label)
        sp.add_errorbar(x,y,yerr=dy,xerr=dx)
        footer_str=''



        html_dict= sp.get_html_draw()


        res_dict = {}
        res_dict['image'] =html_dict
        res_dict['header_text'] = ''
        res_dict['table_text'] = ''
        res_dict['footer_text'] = footer_str


        return res_dict


class SpectrumProduct(BaseQueryProduct):
    def __init__(self, name,
                 data,
                 header,
                 file_name,
                 arf_kw=None,
                 rmf_kw=None,
                 out_arf_file=None,
                 in_arf_file=None,
                 out_rmf_file=None,
                 in_rmf_file=None,
                 **kwargs):

        self.name = name
        self.file_name = file_name

        self.in_arf_file = in_arf_file
        self.in_rmf_file = in_rmf_file

        self.out_arf_file = out_arf_file
        self.out_rmf_file = out_rmf_file

        self.data = data
        self.header = header

        self.arf_kw = arf_kw
        self.rmf_kw = rmf_kw

        self.rmf_file = None
        self.arf_file = None

        self.set_arf_file()
        self.set_rmf_file()

        super(SpectrumProduct, self).__init__(name, file_name=file_name, **kwargs)

    def set_arf_file(self, in_arf_file=None, arf_kw=None, arf_kw_value=None, out_arf_file=None, overwrite=True):

        if in_arf_file is None:
            in_arf_file = self.in_arf_file
        else:
            self.in_arf_file = in_arf_file

        if arf_kw is None:
            arf_kw = self.arf_kw
        else:
            self.arf_kw = arf_kw

        if out_arf_file is None:
            out_arf_file = self.out_arf_file
        else:
            self.out_arf_file = out_arf_file

        if self.header is not None and arf_kw is not None and arf_kw_value is not None:
            self.set_haeder_kw(arf_kw, arf_kw_value)

        if out_arf_file is not None and in_arf_file is not None:
            # print('in_arf_file', in_arf_file,out_arf_file)
            # pf.open(in_arf_file).writeto(out_arf_file, overwrite=overwrite)
            FitsFile(in_arf_file).writeto(out_arf_file, overwrite=overwrite)
            print('arf written to', out_arf_file)

            # if arf_kw is not None  and self.header is not None:
            #    self.header[arf_kw] = out_arf_file
            #    print('set arf kw to', self.header[arf_kw])
        # else:
        # if arf_kw is not None and self.header is not None:
        #    self.header[arf_kw]=self.in_arf_file_path
        #    print('set arf kw to', self.header[arf_kw])

        self.arf_file_path = FilePath(file_name=out_arf_file)
        self.arf_file = out_arf_file

    def set_haeder_kw(self, kw, val):
        if self.header is not None:
            if val is not None and kw is not None:
                self.header[kw] = val

    def del_haeder_kw(self, kw):
        if self.header is not None and kw is not None:
            del self.header[kw]

    def set_rmf_file(self, in_rmf_file=None, rmf_kw=None, rmf_kw_value=None, out_rmf_file=None, overwrite=True):
        if in_rmf_file is None:
            in_rmf_file = self.in_rmf_file
        else:
            self.in_rmf_file = in_rmf_file

        if rmf_kw is None:
            rmf_kw = self.arf_kw
        else:
            self.rmf_kw = rmf_kw

        if out_rmf_file is None:
            out_rmf_file = self.out_rmf_file
        else:
            self.out_rmf_file = out_rmf_file

        if self.header is not None and rmf_kw is not None and rmf_kw_value is not None:
            self.set_haeder_kw(rmf_kw, rmf_kw_value)
        if out_rmf_file is not None and in_rmf_file is not None:
            # pf.open(in_rmf_file).writeto(out_rmf_file, overwrite=overwrite)
            FitsFile(in_rmf_file).writeto(out_rmf_file, overwrite=overwrite)
            print('rmf written to', out_rmf_file)
            # if rmf_kw is not None  and self.header is not None:
            #    self.header[rmf_kw] = out_rmf_file
            #    print('set rmf kw to', self.header[rmf_kw])

        # else:
        #    if rmf_kw is not None and self.header is not None:

        #        self.header[rmf_kw]=self.in_rmf_file
        #        print('set rmf kw to',self.header[rmf_kw])

        self.rmf_file_path = FilePath(file_name=out_rmf_file)
        self.rmf_file = out_rmf_file

    @classmethod
    def from_fits_file(cls, file_name, prod_name, ext=0, arf_file_name=None, rmf_file_name=None):
        # hdu = pf.open(file_name)[ext]
        hdu = FitsFile(file_name).open()[ext]

        data = hdu.data
        header = hdu.header

        return cls(name=prod_name, data=data, header=header, file_name=file_name)

    def write(self, file_name=None, overwrite=True, file_dir=None):
        file_path = self.file_path.get_file_path(file_name=file_name, file_dir=file_dir)
        # print('ciccio')
        FitsFile(file_path).writeto(data=self.data, header=self.header, overwrite=overwrite)
        # pf.writeto(file_path, data=self.data, header=self.header,overwrite=overwrite)


class SpectralFitProduct(BaseQueryProduct):
    def __init__(self, name,
                 spec_file,
                 arf_file,
                 rmf_file,
                 file_dir,
                 **kwargs):

        super(SpectralFitProduct, self).__init__(name, **kwargs)
        self.rmf_file = FilePath(file_name=rmf_file, file_dir=file_dir).path
        self.arf_file = FilePath(file_name=arf_file, file_dir=file_dir).path
        self.spec_file = FilePath(file_name=spec_file, file_dir=file_dir).path
        self.chain_file_path = FilePath(file_name='xspec_fit.chain', file_dir=file_dir)
        self.work_dir = file_dir
        self.out_dir = file_dir

    def parse_command(self, params_setting):
        str_list = params_setting.split('')
        pars_dict = {}
        for s in str_list:
            p = s.split(':')
            if len(p) != 2:
                raise RuntimeError('Malformed par string')
            else:
                i = np.int(p[0])
            pars_dict[i] = p[1]
        return pars_dict

    def set_par(self, m, params_setting):
        if params_setting is not None:
            pars_dict = self.parse_command()
            if pars_dict != {}:
                m.setPars(pars_dict)

    def set_freeze(self, m, frozen_list):
        if frozen_list is not None:
            for f in frozen_list:
                p = f.split(':')
                if len(p) != 2:
                    raise RuntimeError('Malformed freeze string')
                else:
                    comp_name = p[0]
                    par_name = p[1]
                    comp = getattr(m, comp_name)
                    par = getattr(comp, par_name)
                    setattr(par, 'frozen', True)


    def prepare_data(self):
        #do ignore
        #set response/arf
        #ignore bad b
        pass


    def run_fit(self, e_min_kev, e_max_kev, plot=False, xspec_model='powerlaw', params_setting=None, frozen_list=None):
        import xspec as xsp

        xsp.AllModels.clear()
        xsp.AllData.clear()
        xsp.AllChains.clear()
        # PyXspec operations:

        print('fitting->,', self.spec_file)
        print('res', self.rmf_file)
        print('arf', self.arf_file)
        s = xsp.Spectrum(self.spec_file)
        s.response = self.rmf_file.encode('utf-8')
        s.response.arf = self.arf_file.encode('utf-8')

        s.ignore('**-15.')
        s.ignore('300.-**')

        # s.ignore('**-%f'%e_min_kev)
        # s.ignore('%f-**'%e_max_kev)
        xsp.AllData.ignore('bad')

        model_name = xspec_model

        m = xsp.Model(model_name)

        self.set_par(m, params_setting)
        self.set_freeze(m, frozen_list)

        xsp.Fit.query = 'yes'
        xsp.Fit.perform()

        header_str = 'Exposure %f (s)\n' % (s.exposure)
        header_str += 'Fit report for model %s' % (model_name)

        _comp = []
        _name = []
        _val = []
        _unit = []
        _err = []
        colnames = ['component', 'par name', 'value', 'units', 'error']
        for model_name in m.componentNames:
            fit_model = getattr(m, model_name)
            for name in fit_model.parameterNames:
                p = getattr(fit_model, name)
                _comp.append('%s' % (model_name))
                _name.append('%s' % (p.name))
                _val.append('%5.5f' % p.values[0])
                _unit.append('%s' % p.unit)
                _err.append('%5.5f' % p.sigma)

        fit_table = dict(columns_list=[_comp, _name, _val, _unit, _err], column_names=colnames)

        footer_str = 'dof ' + '%d' % xsp.Fit.dof + '\n'
        footer_str += 'Chi-squared ' + '%5.5f\n' % xsp.Fit.statistic
        footer_str += 'Chi-squared red. %5.5f\n\n' % (xsp.Fit.statistic / xsp.Fit.dof)

        try:
            xsp.AllModels.calcFlux("20.0 60.0 err")
            (flux, flux_m, flux_p, _1, _2, _3) = s.flux
            footer_str += 'flux (20.0-60.0) keV %5.5e ergs cm^-2 s^-1\n' % (flux)
            footer_str += 'Error range  68.00%%  confidence (%5.5e,%5.5e) ergs cm^-2 s^-1\n' % (flux_m, flux_p)


        except:
            footer_str += 'flux calculation failed\n'

        _passed = False
        try:
            _passed = True

            if self.chain_file_path.exists():
                self.chain_file_path.remove()

            fit_chain = xsp.Chain(self.chain_file_path.path, burn=500, runLength=1000, algorithm='mh')
            fit_chain.run()
        except:
            footer_str += '!chain failed!\n'

        if _passed:
            try:
                xsp.AllModels.calcFlux("20.0 60.0 err")
                (flux, flux_m, flux_p, _1, _2, _3) = s.flux
                footer_str += '\n'
                footer_str += 'flux calculation with Monte Carlo Markov Chain\n'
                footer_str += 'flux (20.0-60.0) keV %5.5e ergs cm^-2 s^-1\n' % (flux)
                footer_str += 'Error range  68.00%%  confidence (%5.5e,%5.5e) ergs cm^-2 s^-1\n' % (flux_m, flux_p)
            except:
                footer_str += 'flux calculation with Monte Carlo Markov Chain  failed\n'

        _passed = False
        try:
            _passed = True
            xsp.Fit.error('1-%d' % m.nParameters)

            _err_m = []
            _err_p = []
            for model_name in m.componentNames:
                fit_model = getattr(m, model_name)
                for name in fit_model.parameterNames:
                    p = getattr(fit_model, name)
                    _err_m.append('%5.5f' % p.error[0])
                    _err_p.append('%5.5f' % p.error[1])
            fit_table['columns_list'].extend([_err_m, _err_p])
            fit_table['column_names'].extend(['range-', 'range+'])

        except:
            footer_str += '!chain error failed!\n'

        if plot == True:
            xsp.Plot.device = "/xs"

        xsp.Plot.xLog = True
        xsp.Plot.yLog = True
        xsp.Plot.setRebin(10., 10)
        xsp.Plot.xAxis = 'keV'
        # Plot("data","model","resid")
        # Plot("data model resid")
        xsp.Plot("data,delchi")

        if plot == True:
            xsp.Plot.show()

        #import matplotlib.pyplot as plt
        #import matplotlib.gridspec as gridspec
        #gs = gridspec.GridSpec(2, 1, height_ratios=[4, 1])

        #fig = plt.figure()
        #ax1 = fig.add_subplot(gs[0])
        #ax2 = fig.add_subplot(gs[1])

        # fig.subplots_adjust(left=0.05, right=0.95, bottom=0.05, top=0.95,
        #                    hspace=0.1, wspace=0.1)

        x = np.array(xsp.Plot.x())
        y = np.array(xsp.Plot.y())
        dx = np.array(xsp.Plot.xErr())
        dy = np.array(xsp.Plot.yErr())

        mx = x > 0.
        my = y > 0.

        msk = np.logical_and(mx, my)
        msk = np.logical_and(msk, dy > 0.)

        ldx = 0.434 * dx / x
        ldy = 0.434 * dy / y

        y_model = np.array(xsp.Plot.model())

        msk = np.logical_and(msk, y_model > 0.)

        if msk.sum() > 0:

            sp1 = ScatterPlot(w=500, h=350, x_label='Energy (keV)', y_label='normalised counts/s/keV',y_axis_type='log',x_axis_type='log')
                              #y_range=[np.log10(y[msk]).min()-np.log10(y[msk]).min()*0.5,np.log10(y[msk]).max()*1.5])

            sp1.add_errorbar(x[msk], y[msk], yerr=dy[msk], xerr=dx[msk])

            sp1.add_step_line(x[msk], y_model[msk])

            sp2 = ScatterPlot(w=500, h=150, x_label='Energy (keV)', y_label='(data-model)/error',
                              x_range=sp1.fig.x_range,x_axis_type='log',y_axis_type='linear')

            sp2.add_errorbar(x[msk], (y[msk] - y_model[msk]) / dy[msk], yerr=np.ones(msk.sum()),xerr=dx[msk])

            sp2.add_line([x[msk].min(), x[msk].max()], [0, 0])



        else:
            sp1 = ScatterPlot(w=500, h=350, x_label='Energy (keV)', y_label='normalised counts/s/keV',
                              y_axis_type='log', x_axis_type='log')

            sp1.add_errorbar(x, y, yerr=dy, xerr=dx)



            sp2 = ScatterPlot(w=500, h=150, x_label='Energy (keV)', y_label='(data-model)/error',
                              x_range=sp1.fig.x_range, x_axis_type='log', y_axis_type='linear')


            sp2.add_line([x.min(), x.max()], [0, 0])

        #Fixing the missing sp1 error reported by Volodymyr
        gp = GridPlot(sp1, sp2, w=550, h=550)

        htmlt_dict=gp.get_html_draw()

        #print('OK 3')
        xsp.AllModels.clear()
        xsp.AllData.clear()
        xsp.AllChains.clear()

        #if plot == True:
        #    plt.show()

        #plugins.connect(fig, plugins.MousePosition(fontsize=14))

        res_dict = {}
        res_dict['spectral_fit_image'] = htmlt_dict
        res_dict['header_text'] = header_str
        res_dict['table_text'] = fit_table
        res_dict['footer_text'] = footer_str



        return res_dict


class CatalogProduct(BaseQueryProduct):
    def __init__(self, name, catalog, file_name='catalog', **kwargs):
        self.catalog = catalog
        super(CatalogProduct, self).__init__(name, file_name=file_name, **kwargs)

    def write(self, file_name=None, overwrite=True, format='fits', file_dir=None):
        file_path = self.file_path.get_file_path(file_name=file_name, file_dir=file_dir)
        # TODO: this should be file_path = self.file_path.path
        if format !='ds9':

            self.catalog.write(file_path+'.fits', overwrite=overwrite, format=format)
        else :
            self.catalog.write_ds9_region(file_path+'.reg', overwrite=overwrite)



# class MultiSpectralFitProduct(BaseQueryProduct):
#
#     def __init__(self, name,
#                  spectra_list,
#                  file_dir,
#                  **kwargs):
#
#         super(MultiSpectralFitProduct, self).__init__(name, **kwargs)
#         self.spectra_list=spectra_list
#         self.rmf_file = FilePath(file_name=rmf_file, file_dir=file_dir).path
#         self.arf_file = FilePath(file_name=arf_file, file_dir=file_dir).path
#         self.spec_file = FilePath(file_name=spec_file, file_dir=file_dir).path
#         self.chain_file_path = FilePath(file_name='xspec_fit.chain', file_dir=file_dir)
#         self.work_dir = file_dir
#         self.out_dir = file_dir
#
#
# class ProductList():
#     """
#     Container of products
#     """
#     def __init__(self):
#         pass
#
#
#     def add_product(self):
#         pass
#
#
#     def check_product(self):
#         """
#         checks that type is the same
#         :return:
#         """
#         pass
#
#
#     def run(self):
#         pass
