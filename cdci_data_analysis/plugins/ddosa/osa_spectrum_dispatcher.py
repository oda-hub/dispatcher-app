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
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f



# Project
# relative import eg: from .mod import f

from astropy.io import  fits as pf
from pathlib import Path
from .osa_dispatcher import    OsaQuery
from cdci_data_analysis.analysis.queries import SpectrumQuery
from cdci_data_analysis.analysis.products import SpectrumProduct,QueryProductList,QueryOutput
from cdci_data_analysis.analysis.io_helper import FilePath

class IsgriSpectrumProduct(SpectrumProduct):

    def __init__(self,name,file_name,data,header, rmf_file=None, arf_file=None,prod_prefix=None,out_dir=None):
        super(IsgriSpectrumProduct, self).__init__(name,
                                                   data,
                                                   header,
                                                   file_name,
                                                   in_rmf_file=rmf_file,
                                                   in_arf_file=arf_file,
                                                   name_prefix=prod_prefix,
                                                   file_dir=out_dir)

        #check if you need to copy!



    @classmethod
    def build_list_from_ddosa_res(cls,res,prod_prefix=None,out_dir=None):

        data = None
        header=None


        spec_list=[]

        if out_dir is None:
            out_dir='./'
        for source_name, spec_attr, rmf_attr, arf_attr in res.extracted_sources:

            print('spec file-->',getattr(res, spec_attr),spec_attr)
            print('arf file-->', getattr(res, arf_attr), arf_attr)
            print('rmf file-->', getattr(res, rmf_attr), rmf_attr)
            spectrum = pf.open(getattr(res, spec_attr))[1]
            arf_filename= getattr(res, arf_attr)
            rmf_filename = getattr(res, rmf_attr)

            data=spectrum.data
            header=spectrum.header

            file_name=prod_prefix+'_'+Path(getattr(res, spec_attr)).resolve().stem
            #file_name = file_name.replace('+', 'p')
            #file_name = file_name.replace('-', 'm')
            print ('out spec file_name',file_name)

            out_arf_file=prod_prefix+'_'+Path(getattr(res, arf_attr)).name
            out_arf_file=FilePath(file_dir=out_dir,file_name=out_arf_file).path
            print('out arf file_path', out_arf_file)

            out_rmf_file=prod_prefix+'_'+Path(out_dir,getattr(res, rmf_attr)).name
            out_rmf_file = FilePath(file_dir=out_dir,file_name=out_rmf_file).path
            print('out rmf file_path', out_rmf_file)

            name=source_name

            spec= cls(name=name,
                      file_name=file_name,
                      data=data,
                      header=header,
                      rmf_file=rmf_filename,
                      arf_file=arf_filename,
                      out_dir=out_dir)

            spec.set_arf_file(arf_kw='ANCRFILE',out_arf_file=out_arf_file)
            spec.set_rmf_file(rmf_kw='RESPFILE',out_rmf_file=out_rmf_file)
            spec_list.append(spec)

        return spec_list


class OsaSpectrumQuery(SpectrumQuery):

    def __init__(self, name):

        super(OsaSpectrumQuery, self).__init__(name)


    def get_products(self,instrument,job,prompt_delegate,dump_json=False,use_dicosverer=False,config=None,out_dir=None,prod_prefix=None):


        scwlist_assumption, cat, extramodules, inject=OsaQuery.get_osa_query_base(instrument)
        E1=instrument.get_par_by_name('E1_keV').value
        E2=instrument.get_par_by_name('E2_keV').value
        target, modules, assume=self.set_instr_dictionaries(extramodules,scwlist_assumption,E1,E2)
        q=OsaQuery(config=config, target=target, modules=modules, assume=assume, inject=inject)


        res = q.run_query( job=job, prompt_delegate=prompt_delegate)

        if job.status != 'done':
            prod_list = QueryProductList(prod_list=[], job=job)
            return prod_list
        else:
           return self.build_product_list(job,res,out_dir,prod_prefix)


    def set_instr_dictionaries(self,catalog,):
        raise RuntimeError('Must be specified for each instrument')

    def process_product_method(self, instrument, job, prod_list):
        for query_spec in prod_list.prod_list:
            query_spec.write()

        # prod_dictionary = {}
        _names = []
        # _figs=[]
        _files_path = []
        _pf_path = []
        _arf_path = []
        _rmf_path = []
        for query_spec in prod_list.prod_list:
            # print('xspec model',instrument.get_par_by_name('xspec_model').value)
            # _figs.append( query_spec.get_html_draw(plot=False,xspec_model=instrument.get_par_by_name('xspec_model').value))
            _names.append(query_spec.name)
            # _source_spec=[]
            _pf_path.append(str(query_spec.file_path.name))
            _arf_path.append(str(query_spec.arf_file_path.name))
            _rmf_path.append(str(query_spec.rmf_file_path.name))

            # _source_spec.append(query_spec.file_path.get_file_path())
            # _source_spec.append(query_spec.arf_file.encode('utf-8'))
            # _source_spec.append(query_spec.rmf_file.encode('utf-8'))

            # _files_path.append(_source_spec)
            # print ('_source_spec',_source_spec)

        query_out = QueryOutput()

        query_out.prod_dictionary['spectrum_name'] = _names

        query_out.prod_dictionary['ph_file_name'] = _pf_path
        query_out.prod_dictionary['arf_file_name'] = _arf_path
        query_out.prod_dictionary['rmf_file_name'] = _rmf_path

        query_out.prod_dictionary['session_id'] = job.session_id
        query_out.prod_dictionary['job_id'] = job.job_id

        query_out.prod_dictionary['download_file_name'] = 'spectra.tar.gz'
        query_out.prod_dictionary['prod_process_maessage'] = ''

        print('--> send prog')
        return query_out



class IsgriSpectrumQuery(OsaSpectrumQuery):
    def __init__(self,name ):
        super(IsgriSpectrumQuery, self).__init__(name)





    def build_product_list(self,job,res,out_dir,prod_prefix):

        spectrum_list = IsgriSpectrumProduct.build_list_from_ddosa_res(res,
                                                                       out_dir=out_dir,
                                                                       prod_prefix=prod_prefix)

        # print('spectrum_list',spectrum_list)
        prod_list = QueryProductList(prod_list=spectrum_list,job=job)


        return prod_list

    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2):
        target = "ISGRISpectraSum"

        # modules = ["ddosa", "git://ddosadm", "git://useresponse", "git://process_isgri_spectra", "git://rangequery"]

        modules = ["ddosa", "git://ddosadm", "git://useresponse/cd7855bf7", "git://process_isgri_spectra/2200bfd",
                   "git://rangequery"]

        assume = ['process_isgri_spectra.ScWSpectraList(input_scwlist=%s)'% (scwlist_assumption),
                  'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),
                  'process_isgri_spectra.ISGRISpectraSum(use_extract_all=True)',
                  'ddosa.ImagingConfig(use_SouFit=0,use_DoPart2=1,use_version="soufit0_p2")',
                  'ddosa.CatForSpectraFromImaging(use_minsig=3)',
                  ]


        print ('ciccio',target,modules,assume)
        return target,modules,assume


    def get_dummy_products(self,instrument,config,out_dir='./'):

        if out_dir is None:
            out_dir = './'
        import glob,os
        print ('config.dummy_cache',config.dummy_cache)
        print ('out_dir',out_dir)
        spec_files=glob.glob(config.dummy_cache+'/query_spectrum_isgri_sum*.fits')

        print(spec_files)
        spec_list = []
        for spec_file in spec_files:
            src_name=os.path.basename(spec_file)
            src_name=src_name.replace('query_spectrum_isgri_sum_','')
            src_name=src_name.replace('.fits','')
            print ('->',src_name)
            arf_file=glob.glob(config.dummy_cache+'/query_spectrum_arf_sum*%s*.fits.gz'%src_name)[0]
            rmf_file=glob.glob(config.dummy_cache+'/query_spectrum_rmf_sum*%s*.fits.gz'%src_name)[0]
            print('spec file-->', spec_file)
            print('arf file-->', arf_file)
            print('rmf file-->', rmf_file)
            spectrum = pf.open(spec_file)[1]
            arf_filename = arf_file
            rmf_filename = rmf_file

            data = spectrum.data
            header = spectrum.header

            file_name =  Path(spec_file).name
            #file_name = file_name.replace('-', 'm')
            print('out spec file_name', file_name)
            out_arf_file=Path(arf_filename).name
            out_arf_file = str(Path(out_dir,out_arf_file))
            print('out arf file_name', out_arf_file)
            out_rmf_file = Path(rmf_filename).name
            out_rmf_file = str(Path(out_dir,out_rmf_file)).strip()
            print('out rmf file_name', out_rmf_file)

            name = header['NAME']

            spec = IsgriSpectrumProduct(name=name,
                       file_name=file_name,
                       data=data,
                       header=header,
                       rmf_file=rmf_filename,
                       arf_file=arf_filename,
                       out_dir=out_dir)
            spec.set_arf_file(arf_kw='ANCRFILE', out_arf_file=out_arf_file.strip())
            spec.set_rmf_file(rmf_kw='RESPFILE', out_rmf_file=out_rmf_file.strip())
            spec_list.append(spec)





        prod_list = QueryProductList(prod_list=spec_list)

        return prod_list










