from cdci_data_analysis.configurer import DataServerConf
import os
import pytest

def test_data_server_conf(tmpdir, caplog):
    #required_keys = ['data_server_url']
    good_dict = {'data_server_url': 'https://data-server:5000',
                 'dispatcher_mnt_point': 'data',
                 'data_server_remote_cache': 'reduced/ddcache',
                 'dummy_cache': 'data/dummy_prods'
                 }
    bad_dict = {'data_server_url': 'https://data-server:5000',
                 'data_server_port': '5000',
                 'dispatcher_mnt_point': 'data',
                 'data_server_cache': 'reduced/ddcache',
                 'dummy_cache': 'data/dummy_prods'
                 }
    ugly_dict = {'data_server_host': 'data-server',
                 'data_server_port': '5000',
                 'dispatcher_mnt_point': 'data',
                 'data_server_cache': 'reduced/ddcache',
                 'dummy_cache': 'data/dummy_prods'
                 }
    os.chdir(tmpdir)
    conf = DataServerConf.from_conf_dict(good_dict)
    assert conf.dummy_cache == good_dict['dummy_cache']
    assert conf.data_server_url == good_dict['data_server_url']

    conf = DataServerConf.from_conf_dict(bad_dict)
    assert 'disregarded' in caplog.text

    with pytest.raises(KeyError):
        conf = DataServerConf.from_conf_dict(ugly_dict)



