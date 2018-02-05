

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

import json


from pathlib import Path

from astropy import wcs
from astropy.wcs import WCS


from astropy.io  import fits as pf

import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt


import mpld3
from mpld3 import plugins

from .parameters import *


class QueryFilePath(object):
    def __init__(self,file_name,file_dir='./',name_prefix=None):
        if name_prefix is not None:
            file_name=name_prefix+'_'+file_name

        if file_dir is None:
            file_dir='./'
        print ('file_dir,file_name',type(file_dir),type(file_name))
        self.file_path = Path(file_dir, file_name)

    def get_file_path(self,file_name=None,file_dir=None):
        if file_name is  None and file_dir is None:
            file_path=self.file_path
        elif file_name is  None and file_dir is not None:
            file_path= QueryFilePath(file_dir, self.self.file_path.name)
        elif  file_name is not  None and file_dir is  None:
            file_path =  self.file_path.with_name(file_name)
        else:
            file_path= self.file_path

        return str(file_path)



class QueryOutput(object):
    def  __init__(self):
        self.prod_dictionary={}
        self.status_dictionary={}

    
    def set_products(self,keys,values):
        for k,v in zip(keys,values):
            self.prod_dictionary[k] =v
    
    def set_status(self,status,error_message='',debug_message=''):
       

        self.status_dictionary['status']=status
        self.status_dictionary['error_message']=str(error_message)
        self.status_dictionary['debug_message']=str(debug_message)

    
class QueryProductList(object):

    def __init__(self,prod_list,job=None):
        self._prod_list=prod_list
        self.job=job

    @property
    def prod_list(self):
        return  self._prod_list

    def get_prod_by_name(self,name):
        prod=None
        for prod1 in self._prod_list:
            if hasattr(prod1,'name'):
                if prod1.name==name:
                    prod=prod1
        if prod is None:
            raise  Warning('product',name,'not found')
        return prod

class BaseQueryProduct(object):


    def __init__(self,name,file_name=None,file_dir='./',name_prefix=None):
        self.name=name
        if file_name is not None:

            print ('set file phat')
            print ('workig dir',file_dir)
            print ('file name',file_name)
            print ('name_prefix',name_prefix)
            self.file_path=QueryFilePath(file_name,file_dir=file_dir,name_prefix=name_prefix)
            print('file_path set to',self.file_path.get_file_path())

    def write(self):
        pass


    def read(self):
        pass



class ImageProduct(BaseQueryProduct):
    def __init__(self,name,data,header,file_name='image.fits',**kwargs):
        self.name=name
        self.data=data
        self.header=header
        self.file_name = file_name
        super(ImageProduct, self).__init__(name,file_name=file_name, **kwargs)

    @classmethod
    def from_fits_file(cls,in_file,out_file_name,prod_name,ext=0,**kwargs):
        hdu = pf.open(in_file)[ext]
        data = hdu.data
        header = hdu.header

        return  cls(name=prod_name, data=data, header=header,file_name=out_file_name,**kwargs)

    def write(self,file_name=None,overwrite=True,file_dir=None):

        file_path=self.file_path.get_file_path(file_name=file_name,file_dir=file_dir)
        pf.writeto( file_path   , data=self.data, header=self.header,overwrite=overwrite)

    def get_html_draw(self, catalog=None,plot=False,vmin=None,vmax=None):
        #print('vmin,vmax',vmin,vmax)
        msk=~np.isnan(self.data)
        if vmin is None:
            vmin=self.data[msk].min()

        if vmax is None:
            vmax=self.data[msk].max()

        fig, (ax) = plt.subplots(1, 1, figsize=(4, 3), subplot_kw={'projection': WCS(self.header)})
        im = ax.imshow(self.data,
                       origin='lower',
                       zorder=1,
                       interpolation='none',
                       aspect='equal',
                       cmap=plt.get_cmap('jet'),
                       vmin = vmin,
                       vmax = vmax)

        if catalog is not None:

            lon = catalog.ra
            lat = catalog.dec

            w = wcs.WCS(self.header)
            if len(lat)>0.:
                pixcrd = w.wcs_world2pix(np.column_stack((lon, lat)), 0)

                msk=~np.isnan(pixcrd[:, 0])
                ax.plot(pixcrd[:, 0][msk], pixcrd[:, 1][msk], 'o', mfc='none')

                for ID, (x, y) in enumerate(pixcrd):
                    if msk[ID]:
                        #print ('xy',(pixcrd[:, 0][ID], pixcrd[:, 1][ID]))
                        ax.annotate('%s' % catalog.name[ID], xy=(x,y), color='white')


        ax.set_xlabel('RA')
        ax.set_ylabel('DEC')
        ax.grid(True, color='white')
        fig.colorbar(im, ax=ax)


        plugins.connect(fig, plugins.MousePosition(fontsize=14))
        if plot == True:
            print ('plot',plot)
            mpld3.show()
        res_dict = {}
        res_dict['image'] =  mpld3.fig_to_dict(fig)
        res_dict['header_text'] = ''
        res_dict['table_text'] = ''
        res_dict['footer_text'] = 'colorscale for normalzied significance\nmax significance=%.2f, min significance=%.2f'%(vmax,vmin)

        plt.close(fig)
        return res_dict


class LightCurveProduct(BaseQueryProduct):
    def __init__(self,name,
                      data,
                      header,
                      file_name='lc.fits',
                      src_name=None,
                      **kwargs):
        self.name = name
        self.data = data
        self.header = header
        self.file_name = file_name
        self.src_name=src_name

        super(LightCurveProduct, self).__init__(name,file_name=file_name,**kwargs)

    @classmethod
    def from_fits_file(cls, inf_file,out_file_name, prod_name, ext=0,**kwargs):
        hdu = pf.open(inf_file)[ext]
        data = hdu.data
        header = hdu.header
        return cls(name=prod_name, data=data, header=header, file_name=out_file_name,**kwargs)

    def write(self, file_name=None, overwrite=True,file_dir=None):
        file_path = self.file_path.get_file_path(file_name=file_name, file_dir=file_dir)
        pf.writeto(file_path, data=self.data, header=self.header, overwrite=overwrite)

    def get_html_draw(self, plot=False):
        from astropy.io import fits as pf
        hdul = pf.open(self.file_path.get_file_path())

        data = hdul[1].data
        header = hdul[1].header

        import matplotlib
        matplotlib.use('TkAgg')
        import pylab as plt
        fig, ax = plt.subplots()
        x = data['TIME']
        y = data['RATE']
        dy = data['ERROR']
        mjdref = header['mjdref'] + np.int(x.min())

        x = x - np.int(x.min())
        plt.errorbar(x, y, yerr=dy, fmt='o')
        ax.set_xlabel('MJD-%d  (days)' % mjdref)
        ax.set_ylabel('Rate  (cts/s)')

        slope = None
        normalized_slope = None
        chisq_red = None
        poly_deg = 0
        footer_str=''
        p, chisq, chisq_red, dof=self.do_linear_fit( x, y, dy, poly_deg,'constant fit')

        exposure=header['TIMEDEL']*data['FRACEXP'].sum()
        exposure*=86400
        footer_str='Exposure %5.5f (s) \n'%exposure
        if p is not None:
            footer_str +='\n'
            footer_str += 'Constant fit\n'
            footer_str += 'flux level %5.5f\n'%p[0]
            footer_str += 'dof ' + '%d' % dof + '\n'
            footer_str += 'Chi-squared red. %5.5f\n' % chisq_red

        poly_deg=1
        p, chisq, chisq_red, dof = self.do_linear_fit( x, y, dy, poly_deg,'linear fit')
        if p is not None:
            footer_str += '\n'
            footer_str += 'Linear fit\n'
            footer_str += 'slope %5.5f\n'%p[0]
            footer_str += 'dof ' + '%d' % dof + '\n'
            footer_str += 'Chi-squared red. %5.5f\n' % chisq_red

        ax.legend(loc='best')

        if plot == True:
            plt.show()


        plugins.connect(fig, plugins.MousePosition(fontsize=14))




        res_dict = {}
        res_dict['image'] = mpld3.fig_to_dict(fig)
        res_dict['header_text'] = ''
        res_dict['table_text'] =  ''
        res_dict['footer_text'] = footer_str

        plt.close(fig)
        return res_dict

    def do_linear_fit(self,x,y,dy,poly_deg,label):
        p=None
        chisq=None
        chisq_red=None
        dof=None

        if y.size > poly_deg + 1:
            p = np.polyfit(x, y, poly_deg)

            x_grid = np.linspace(x.min(), x.max(), 100)
            lin_fit = np.poly1d(p)

            chisq = (lin_fit(x) - y) ** 2 / dy ** 2
            dof = y.size - (poly_deg + 1)
            chisq_red = chisq.sum() / float(dof)
            plt.plot(x_grid, lin_fit(x_grid), '--',label=label)




        return p,chisq,chisq_red,dof


class   SpectrumProduct(BaseQueryProduct):
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

        self.name=name
        self.file_name=file_name

        self.in_arf_file=in_arf_file
        self.in_rmf_file=in_rmf_file

        self.out_arf_file = out_arf_file
        self.out_rmf_file = out_rmf_file

        self.data = data
        self.header = header

        self.arf_kw=arf_kw
        self.rmf_kw = rmf_kw

        self.rmf_file=None
        self.arf_file=None


        self.set_arf_file()
        self.set_rmf_file()


        super(SpectrumProduct, self).__init__(name,file_name=file_name, **kwargs)

    def set_arf_file(self, in_arf_file=None,arf_kw=None, out_arf_file=None, overwrite=True):

        if in_arf_file is None:
            in_arf_file=self.in_arf_file
        else:
            self.in_arf_file=in_arf_file

        if arf_kw is None:
            arf_kw=self.arf_kw
        else:
            self.arf_kw=arf_kw

        if out_arf_file is None:
            out_arf_file=self.out_arf_file
        else:
            self.out_arf_file=out_arf_file

        if self.header is not None and arf_kw is not None:
            print("kw -->",arf_kw)
            print("-->", self.header[arf_kw])
            self.header[arf_kw] = 'NONE'
        if out_arf_file is not None and in_arf_file is not None:
            pf.open(in_arf_file).writeto(out_arf_file, overwrite=overwrite)
            print('arf written to', out_arf_file)

            #if arf_kw is not None  and self.header is not None:
            #    self.header[arf_kw] = out_arf_file
            #    print('set arf kw to', self.header[arf_kw])
        #else:
            #if arf_kw is not None and self.header is not None:
            #    self.header[arf_kw]=self.in_arf_file_path
            #    print('set arf kw to', self.header[arf_kw])

        self.arf_file=out_arf_file


    def set_rmf_file(self, in_rmf_file=None,rmf_kw=None, out_rmf_file=None, overwrite=True):
        if in_rmf_file is None:
            in_rmf_file=self.in_rmf_file
        else:
            self.in_rmf_file=in_rmf_file

        if rmf_kw is None:
            rmf_kw=self.arf_kw
        else:
            self.rmf_kw=rmf_kw

        if out_rmf_file is None:
            out_rmf_file=self.out_rmf_file
        else:
            self.out_rmf_file=out_rmf_file

        if self.header is not None and rmf_kw is not None:
            print("kw -->", rmf_kw)
            print ("-->", self.header[rmf_kw] )
            self.header[rmf_kw] = 'NONE'
        if out_rmf_file is not None and in_rmf_file is not None:
            pf.open(in_rmf_file).writeto(out_rmf_file, overwrite=overwrite)
            print('rmf written to', out_rmf_file)
            #if rmf_kw is not None  and self.header is not None:
            #    self.header[rmf_kw] = out_rmf_file
            #    print('set rmf kw to', self.header[rmf_kw])

        #else:
        #    if rmf_kw is not None and self.header is not None:

        #        self.header[rmf_kw]=self.in_rmf_file
        #        print('set rmf kw to',self.header[rmf_kw])

        self.rmf_file = out_rmf_file


    @classmethod
    def from_fits_file(cls, file_name, prod_name, ext=0,arf_file_name=None,rmf_file_name=None):
        hdu = pf.open(file_name)[ext]
        data = hdu.data
        header = hdu.header

        return cls(name=prod_name, data=data, header=header, file_name=file_name)

    def write(self,file_name=None,overwrite=True,file_dir=None):
        file_path = self.file_path.get_file_path(file_name=file_name, file_dir=file_dir)

        pf.writeto(file_path, data=self.data, header=self.header,overwrite=overwrite)


    def get_html_draw(self, catalog=None, plot=False,xspec_model='powerlaw'):
        import xspec as xsp
        xsp.AllModels.clear()
        xsp.AllData.clear()
        xsp.AllChains.clear()
        # PyXspec operations:
        file_path=self.file_path.get_file_path()
        print('fitting->,',file_path)
        print('res',self.rmf_file,type(self.rmf_file.encode('utf-8')))
        print('arf',self.arf_file,type(self.arf_file.encode('utf-8')))
        s = xsp.Spectrum(file_path)
        s.response = self.rmf_file.encode('utf-8')
        s.response.arf=self.arf_file.encode('utf-8')

        s.ignore('**-15.')
        s.ignore('300.-**')

        model_name=xspec_model

        m = xsp.Model(model_name)
        xsp.Fit.query = 'yes'
        xsp.Fit.perform()

        header_str='Exposure %f (s)\n'%(s.exposure)
        header_str +='Fit report for model %s' % (model_name)


        _comp=[]
        _name=[]
        _val=[]
        _unit=[]
        _err=[]
        colnames=['component','par name','value','units','error']
        for model_name in m.componentNames:
            fit_model = getattr(m, model_name)
            for name in fit_model.parameterNames:
                p=getattr(fit_model,name)
                _comp.append('%s' % (model_name))
                _name.append('%s'%(p.name))
                _val.append('%5.5f'%p.values[0])
                _unit.append('%s'%p.unit)
                _err.append('%5.5f'%p.sigma)

        fit_table=dict(columns_list=[_comp,_name,_val,_unit,_err], column_names=colnames)

        footer_str ='dof '+ '%d'%xsp.Fit.dof+'\n'

        footer_str +='Chi-squared '+ '%5.5f\n'%xsp.Fit.statistic
        footer_str +='Chi-squared red. %5.5f\n'%(xsp.Fit.statistic/xsp.Fit.dof)

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

        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
        gs = gridspec.GridSpec(2, 1, height_ratios=[4, 1])

        fig = plt.figure()
        ax1 = fig.add_subplot(gs[0])
        ax2 = fig.add_subplot(gs[1])

        # fig.subplots_adjust(left=0.05, right=0.95, bottom=0.05, top=0.95,
        #                    hspace=0.1, wspace=0.1)

        x = np.array(xsp.Plot.x())
        y = np.array(xsp.Plot.y())
        dx = np.array(xsp.Plot.xErr())
        dy = np.array(xsp.Plot.yErr())

        mx = x > 0.
        my = y > 0.

        msk = np.logical_and(mx, my)
        msk=  np.logical_and(msk,dy>0.)



        ldx = 0.434 * dx / x
        ldy = 0.434 * dy / y

        y_model = np.array(xsp.Plot.model())

        msk = np.logical_and(msk, y_model > 0.)

        if msk.sum()>0:
            ax1.errorbar(np.log10(x[msk]), np.log10(y[msk]), xerr=ldx[msk], yerr=ldy[msk], fmt='o')
            ax1.step(np.log10(x[msk]), np.log10(y_model[msk]), where='mid')

            # ax1.set_xlabel('log (Energy (keV))')
            ax1.set_ylabel('log (normalize counts/s/keV)')
            # ax1.set_ylim(-3,1)
            ax2.errorbar(np.log10(x[msk]), (y[msk] - y_model[msk]) / dy[msk], yerr=1., xerr=0., fmt='o')
            ax2.plot(ax1.get_xlim(), [0., 0.], '--')
            ax1.set_ylim(np.log10(y[msk]).min() - 0.5, np.log10(y[msk]).max() + 0.5)
            ax2.set_xlim(ax1.get_xlim())
            ax2.set_ylabel('(data-model)/error')
            ax2.set_xlabel('log (Energy) (keV)')



        xsp.AllModels.clear()
        xsp.AllData.clear()
        xsp.AllChains.clear()

        if plot == True:
            plt.show()

        plugins.connect(fig, plugins.MousePosition(fontsize=14))

        res_dict={}
        res_dict['image']= mpld3.fig_to_dict(fig)
        res_dict['header_text']=header_str
        res_dict['table_text'] = fit_table
        res_dict['footer_text'] = footer_str

        plt.close(fig)

        return res_dict


class SpectralFitProduct(BaseQueryProduct):
    def __init__(self, name,
                 spec_file,
                 arf_file,
                 rmf_file,
                 file_dir,
                 **kwargs):


        super(SpectralFitProduct, self).__init__(name, **kwargs)
        self.rmf_file = QueryFilePath(file_name=rmf_file, file_dir=file_dir).get_file_path()
        self.arf_file = QueryFilePath(file_name=arf_file, file_dir=file_dir).get_file_path()
        self.spec_file = QueryFilePath(file_name=spec_file, file_dir=file_dir).get_file_path()





    def run_fit(self, plot=False,xspec_model='powerlaw'):
        import xspec as xsp
        xsp.AllModels.clear()
        xsp.AllData.clear()
        xsp.AllChains.clear()
        # PyXspec operations:

        print('fitting->,',self.spec_file)
        print('res',self.rmf_file)
        print('arf',self.arf_file)
        s = xsp.Spectrum(self.spec_file)
        s.response = self.rmf_file.encode('utf-8')
        s.response.arf=self.arf_file.encode('utf-8')

        s.ignore('**-15.')
        s.ignore('300.-**')
        xsp.AllData.ignore('bad')

        model_name=xspec_model

        m = xsp.Model(model_name)
        xsp.Fit.query = 'yes'
        xsp.Fit.perform()

        header_str='Exposure %f (s)\n'%(s.exposure)
        header_str +='Fit report for model %s' % (model_name)


        _comp=[]
        _name=[]
        _val=[]
        _unit=[]
        _err=[]
        colnames=['component','par name','value','units','error']
        for model_name in m.componentNames:
            fit_model = getattr(m, model_name)
            for name in fit_model.parameterNames:
                p=getattr(fit_model,name)
                _comp.append('%s' % (model_name))
                _name.append('%s'%(p.name))
                _val.append('%5.5f'%p.values[0])
                _unit.append('%s'%p.unit)
                _err.append('%5.5f'%p.sigma)

        fit_table=dict(columns_list=[_comp,_name,_val,_unit,_err], column_names=colnames)

        footer_str ='dof '+ '%d'%xsp.Fit.dof+'\n'

        footer_str +='Chi-squared '+ '%5.5f\n'%xsp.Fit.statistic
        footer_str +='Chi-squared red. %5.5f\n'%(xsp.Fit.statistic/xsp.Fit.dof)

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

        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
        gs = gridspec.GridSpec(2, 1, height_ratios=[4, 1])

        fig = plt.figure()
        ax1 = fig.add_subplot(gs[0])
        ax2 = fig.add_subplot(gs[1])

        # fig.subplots_adjust(left=0.05, right=0.95, bottom=0.05, top=0.95,
        #                    hspace=0.1, wspace=0.1)

        x = np.array(xsp.Plot.x())
        y = np.array(xsp.Plot.y())
        dx = np.array(xsp.Plot.xErr())
        dy = np.array(xsp.Plot.yErr())

        mx = x > 0.
        my = y > 0.

        msk = np.logical_and(mx, my)
        msk=  np.logical_and(msk,dy>0.)



        ldx = 0.434 * dx / x
        ldy = 0.434 * dy / y

        y_model = np.array(xsp.Plot.model())

        msk = np.logical_and(msk, y_model > 0.)

        if msk.sum()>0:
            ax1.errorbar(np.log10(x[msk]), np.log10(y[msk]), xerr=ldx[msk], yerr=ldy[msk], fmt='o')
            ax1.step(np.log10(x[msk]), np.log10(y_model[msk]), where='mid')

            # ax1.set_xlabel('log (Energy (keV))')
            ax1.set_ylabel('log (normalize counts/s/keV)')
            # ax1.set_ylim(-3,1)
            ax2.errorbar(np.log10(x[msk]), (y[msk] - y_model[msk]) / dy[msk], yerr=1., xerr=0., fmt='o')
            ax2.plot(ax1.get_xlim(), [0., 0.], '--')
            ax1.set_ylim(np.log10(y[msk]).min() - 0.5, np.log10(y[msk]).max() + 0.5)
            ax2.set_xlim(ax1.get_xlim())
            ax2.set_ylabel('(data-model)/error')
            ax2.set_xlabel('log (Energy) (keV)')



        xsp.AllModels.clear()
        xsp.AllData.clear()
        xsp.AllChains.clear()

        if plot == True:
            plt.show()

        plugins.connect(fig, plugins.MousePosition(fontsize=14))

        res_dict={}
        res_dict['spectral_fit_image']= mpld3.fig_to_dict(fig)
        res_dict['header_text']=header_str
        res_dict['table_text'] = fit_table
        res_dict['footer_text'] = footer_str

        plt.close(fig)

        return res_dict


class CatalogProduct(BaseQueryProduct):
    def __init__(self, name,catalog,file_name='catalog.fits', **kwargs):
        self.catalog=catalog
        super(CatalogProduct, self).__init__(name,file_name=file_name, **kwargs)


    def write(self,file_name=None,overwrite=True,format='fits',file_dir=None):
        file_path = self.file_path.get_file_path(file_name=file_name, file_dir=file_dir)
        self.catalog.write(file_path,overwrite=overwrite,format=format)
