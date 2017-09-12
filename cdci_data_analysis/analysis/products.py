

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


class QueryProductList(object):

    def __init__(self,prod_list):
        self._prod_list=prod_list

    @property
    def prod_list(self):
        return  self._prod_list

    def get_prod_by_name(self,name):
        prod=None
        for prod1 in self._prod_list:
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

    def get_html_draw(self, catalog=None,plot=False):


        fig, (ax) = plt.subplots(1, 1, figsize=(4, 3), subplot_kw={'projection': WCS(self.header)})
        im = ax.imshow(self.data, origin='lower', zorder=1, interpolation='none', aspect='equal')

        if catalog is not None:

            lon = catalog.ra
            lat = catalog.dec

            w = wcs.WCS(self.header)
            pixcrd = w.wcs_world2pix(np.column_stack((lon, lat)), 1)
            
            msk=~np.isnan(pixcrd[:, 0])
            ax.plot(pixcrd[:, 0][msk], pixcrd[:, 1][msk], 'o', mfc='none')

            for ID, (x, y) in enumerate(pixcrd):
                if msk[ID]:
                    #print ('xy',(pixcrd[:, 0][ID], pixcrd[:, 1][ID]))
                    ax.annotate('%s' % catalog.name[ID], xy=(x,y), color='white')
                            

            ax.set_xlabel('RA')
            ax.set_ylabel('DEC')

        fig.colorbar(im, ax=ax)
        if plot == True:
            plt.show()

        plugins.connect(fig, plugins.MousePosition(fontsize=14))

        return mpld3.fig_to_dict(fig)


class LightCurveProduct(BaseQueryProduct):
    def __init__(self,name,
                      data,
                      header,
                      file_name='lc.fits',
                      **kwargs):
        self.name = name
        self.data = data
        self.header = header
        self.file_name = file_name


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
        data= pf.getdata(self.file_path.get_file_path(),ext=1)

        import matplotlib
        matplotlib.use('TkAgg')
        import pylab as plt
        fig, ax = plt.subplots()

        #ax.set_xscale("log", nonposx='clip')
        #ax.set_yscale("log")

        plt.errorbar(data['TIME'], data['RATE'], yerr=data['ERROR'], fmt='o')
        ax.set_xlabel('Time ')
        ax.set_ylabel('Rate ')

        if plot == True:
            plt.show()

        plugins.connect(fig, plugins.MousePosition(fontsize=14))

        return mpld3.fig_to_dict(fig)


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


    def get_html_draw(self, catalog=None, plot=False):
        import xspec as xsp
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

        model_name='cutoffpl'

        m = xsp.Model(model_name)
        xsp.Fit.query = 'yes'
        xsp.Fit.perform()

        header_str='Exposure %f (s)\n'%s.exposure


        fit_model = getattr(m, model_name)

        header_str = 'Model %s\n' % (getattr(m, model_name))
        header_str += 'Fit report\n'
        _name=[]
        _val=[]
        _unit=[]
        _err=[]
        colnames=['par name','value','units','error']
        for name in fit_model.parameterNames:
            p=getattr(fit_model,name)
            _name.append(p.name)
            _val.append('%5.5f'%p.values[0])
            _unit.append(' %s'%p.unit)
            _err.append('%5.5f'%p.sigma)

        fit_table=dict(columns_list=[_name,_val,_unit,_err], column_names=colnames)

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
        mx = x > 0
        my = y > 0
        msk = np.logical_and(mx, my)

        ldx = 0.434 * dx / x
        ldy = 0.434 * dy / y

        y_model = np.array(xsp.Plot.model())

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

        return res_dict


class CatalogProduct(BaseQueryProduct):
    def __init__(self, name,catalog,file_name='catalog.fits', **kwargs):
        self.catalog=catalog
        super(CatalogProduct, self).__init__(name,file_name=file_name, **kwargs)


    def write(self,file_name=None,overwrite=True,format='fits',file_dir=None):
        file_path = self.file_path.get_file_path(file_name=file_name, file_dir=file_dir)
        self.catalog.write(file_path,overwrite=overwrite,format=format)
