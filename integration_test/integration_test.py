import pytest   

from oda_api.api import DispatcherAPI
from oda_api.plot_tools import OdaImage,OdaLightCurve
from oda_api.data_products import BinaryData
import os


def integration_test():

    disp=DispatcherAPI(host='10.194.169.161',port=32784,instrument='mock')

    
    


    # In[4]:


    instr_list=disp.get_instruments_list()
    for i in instr_list:
        print (i)


    # In[ ]:





    # ### get the description of the instrument

    # In[5]:


    disp.get_instrument_description('isgri')


    # ### get the description of the product
    # 

    # In[6]:


    disp.get_product_description(instrument='isgri',product_name='isgri_image')


    # ## Get ODA products
    # now we skip the dry_run to actually get the products

    # In[8]:


    data=disp.get_product(instrument='isgri',
                        product='isgri_image',
                        T1='2003-03-15T23:27:40.0',
                        T2='2003-03-16T00:03:15.0',
                        E1_keV=20.0,
                        E2_keV=40.0,
                        osa_version='OSA10.2',
                        RA=255.986542,
                        DEC=-37.844167,
                        detection_threshold=5.0,
                        radius=15.,
                        product_type='Real')


    # ### the ODA data structure

    # In[9]:


    data.show()


    # you can acess memeber by name:

    # In[10]:


    data.mosaic_image_0


    # or by position in the data list

    # In[11]:


    data._p_list[0]


    # ### the ODA catalog

    # In[12]:


    data.dispatcher_catalog_1.table


    # you can use astropy.table commands to modify the table of the catatlog http://docs.astropy.org/en/stable/table/modify_table.html

    # to generate a catalog to pass to the dispatcher api

    # In[13]:


    api_cat=data.dispatcher_catalog_1.get_api_dictionary()


    # In[14]:


    api_cat


    # In[15]:


    data=disp.get_product(instrument='isgri',
                        product='isgri_image',
                        T1='2003-03-15T23:27:40.0',
                        T2='2003-03-16T00:03:15.0',
                        E1_keV=20.0,
                        E2_keV=40.0,
                        osa_version='OSA10.2',
                        RA=255.986542,
                        DEC=-37.844167,
                        detection_threshold=5.0,
                        radius=15.,
                        product_type='Real',
                        selected_catalog=api_cat)


    # you can explore the image with the following command

    # In[16]:


    data.mosaic_image_0.show()


    # In[17]:


    data.mosaic_image_0.show_meta()


    # In[23]:


    data.mosaic_image_0.data_unit[1].data


    # In[24]:


    hdu=data.mosaic_image_0.to_fits_hdu_list()


    # In[25]:


    data.mosaic_image_0.data_unit[1].data.shape


    # In[26]:


    data.mosaic_image_0.write_fits_file('test.fits',overwrite=True)


    # ### the ODA Image   plotting tool

    # In[34]:


    #interactive
    #%matplotlib notebook

    im=OdaImage(data.mosaic_image_0)


    # In[35]:


    im.show(unit_ID=1)


    # In[33]:


    data.mosaic_image_0.data_unit[1].header


    # ### the ODA LC  plotting tool

    # In[36]:


    data=disp.get_product(instrument='isgri',
                        product='isgri_lc',
                        T1='2003-03-15T23:27:40.0',
                        T2='2003-03-16T00:03:12.0',
                        time_bin=70,
                        osa_version='OSA10.2',
                        RA=255.986542,
                        DEC=-37.844167,
                        detection_threshold=5.0,
                        radius=15.,
                        product_type='Real')


    # ### explore LC

    # In[37]:


    data.show()


    # In[38]:


    data.isgri_lc_0.show_meta()


    # In[39]:


    for ID,s in enumerate(data._p_list):
        print (ID,s.meta_data['src_name'])


    # In[40]:


    lc=data._p_list[0]
    lc.data_unit[1].data


    # In[41]:


    lc.show()


    # In[42]:


    lc.meta_data


    # In[43]:


    #get_ipython().run_line_magic('matplotlib', 'inline')
    #OdaLightCurve(lc).show(unit_ID=1)


    # In[44]:


    lc.data_unit[0].header


    # ### Polar LC

    # In[45]:


    #conda create --name=polar_root root=5 python=3 -c nlesc
    #source activate poloar_root
    #conda install astropy future -c nlesc
    #conda install -c conda-forge json_tricks
    #from oda_api.api import DispatcherAPI
    #from oda_api.data_products import BinaryData
    #from oda_api.plot_tools import OdaImage,OdaLightCurve
    #disp=DispatcherAPI(host='10.194.169.161',port=32784,instrument='mock',protocol='http')
    data=disp.get_product(instrument='polar',product='polar_lc',T1='2016-12-18T08:32:21.000',T2='2016-12-18T08:34:01.000',time_bin=0.5,verbose=True,dry_run=False)


    # In[46]:


    data.show()


    # In[47]:


    data._p_list[0]


    # In[48]:


    lc=data._p_list[0]
    root=data._p_list[1]
    open('lc.root', "wb").write(root)


    # In[49]:


    open('lc.root', "wb").write(root)


    # In[51]:


    #get_ipython().run_line_magic('matplotlib', 'inline')
    #OdaLightCurve(lc).show(unit_ID=1)


    # ### SPIACS LC

    # In[52]:


    disp.get_instrument_description('spi_acs')


    # In[53]:


    data=disp.get_product(instrument='spi_acs',
                        product='spi_acs_lc',
                        T1='2003-03-15T23:27:40.0',
                        T2='2003-03-15T23:57:12.0',
                        time_bin=2,
                        osa_version='OSA10.2',
                        RA=255.986542,
                        DEC=-37.844167,
                        detection_threshold=5.0,
                        radius=15.,
                        product_type='Real')


    # In[54]:


    data.show()


    # In[55]:


    lc=data._p_list[0]


    # In[56]:


    lc.show()


    # In[58]:


    lc.data_unit[1].header


    # In[59]:


    lc.data_unit[1].data[0:10]


    # In[60]:


    #OdaLightCurve(lc).show(unit_ID=1)


    # ### the ODA  and spectra

    # In[61]:


    data=disp.get_product(instrument='isgri',
                        product='isgri_spectrum',
                        T1='2003-03-15T23:27:40.0',
                        T2='2003-03-16T00:03:12.0',
                        time_bin=50,
                        osa_version='OSA10.2',
                        RA=255.986542,
                        DEC=-37.844167,
                        detection_threshold=5.0,
                        radius=15.,
                        product_type='Real')


    # ### explore spectra

    # In[62]:


    for ID,s in enumerate(data._p_list):
        print (ID,s.meta_data)


    # In[63]:


    data._p_list[87].write_fits_file('spec.fits')
    data._p_list[88].write_fits_file('arf.fits')
    data._p_list[89].write_fits_file('rmf.fits')


    # In[64]:


    s.show()


    # In[65]:


    d=data._p_list[3]


    # In[ ]:





    # In[66]:


    d.data_unit[1].header


    # ### JEM-X test

    # In[67]:


    disp.get_instrument_description('jemx')


    # In[68]:


    data=disp.get_product(instrument='jemx',
                        jemx_num='2',
                        product='jemx_image',
                        scw_list=['010200230010.001'],
                        osa_version='OSA10.2',
                        detection_threshold=5.0,
                        radius=15.,
                        product_type='dummy')


    # In[69]:


    data=disp.get_product(instrument='jemx',
                        jemx_nume='2',
                        product='jemx_lc',
                        scw_list=['010200230010.001'],
                        osa_version='OSA10.2',
                        detection_threshold=5.0,
                        radius=15.,
                        product_type='Real')


    # In[70]:


    data=disp.get_product(instrument='jemx',
                        jemx_num='2',
                        product='jemx_spectrum',
                        scw_list=['010200230010.001'],
                        osa_version='OSA10.2',
                        detection_threshold=5.0,
                        radius=15.,
                        product_type='Real')



integration_test()