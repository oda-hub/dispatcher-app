import pytest

from cdci_data_analysis.pytest_fixtures import (
            app, 
            dispatcher_local_mail_server,
            dispatcher_local_mail_server_subprocess,
            dispatcher_live_fixture,
            dispatcher_live_fixture_no_debug_mode,
            dispatcher_long_living_fixture,
            dispatcher_test_conf,
            dispatcher_test_conf_empty_sentry_fn,
            dispatcher_test_conf_fn,
            dispatcher_debug,
            dispatcher_nodebug,
            cleanup,
            empty_products_files_fixture,
            empty_products_user_files_fixture,
            default_params_dict,
            default_token_payload,
            dispatcher_live_fixture_empty_sentry
        )


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "dda" in item.keywords:
            item.add_marker(skip_slow)
