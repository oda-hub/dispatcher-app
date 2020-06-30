import pytest

import cdci_data_analysis.flask_app.app

@pytest.fixture(scope="session")
def app():
    app = cdci_data_analysis.flask_app.app
    return app
