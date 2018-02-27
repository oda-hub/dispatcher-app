from cdci_data_analysis.ddosa_interface.osa_catalog import OsaIsgriCatalog

def test_from_list():
    osa_catalog=OsaIsgriCatalog.build_from_dict_list([dict(ra=0,dec=0,name="SOURCE_NAME")])
    osa_catalog.write("osa_cat_write_test.fits",format="fits")

    osa_catalog_read=OsaIsgriCatalog.from_fits_file("osa_cat_write_test.fits")

    assert osa_catalog.name == osa_catalog_read.name