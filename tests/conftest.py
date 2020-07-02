import pytest

import cdci_data_analysis.flask_app.app
from cdci_data_analysis.configurer import ConfigEnv

@pytest.fixture(scope="session")
def app():
    app = cdci_data_analysis.flask_app.app.app
    app.config['conf'] = ConfigEnv.from_conf_file("cdci_data_analysis/config_dir/conf_env.yml")
    return app
