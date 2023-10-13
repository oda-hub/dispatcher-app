import pytest
import pytest_sentry

from cdci_data_analysis.pytest_fixtures import (
            app, 
            dispatcher_local_mail_server,
            dispatcher_local_mail_server_subprocess,
            dispatcher_local_matrix_message_server,
            dispatcher_live_fixture,
            gunicorn_dispatcher_live_fixture,
            dispatcher_live_fixture_no_debug_mode,
            dispatcher_live_fixture_with_gallery,
            dispatcher_live_fixture_with_gallery_no_resolver,
            dispatcher_long_living_fixture,
            gunicorn_dispatcher_long_living_fixture,
            dispatcher_long_living_fixture_with_matrix_options,
            dispatcher_no_bcc_matrix_room_ids,
            gunicorn_dispatcher_long_living_fixture_with_matrix_options,
            dispatcher_test_conf,
            dispatcher_test_conf_with_gallery,
            dispatcher_test_conf_with_gallery_no_resolver,
            dispatcher_test_conf_empty_sentry_fn,
            dispatcher_test_conf_with_gallery_fn,
            dispatcher_test_conf_with_gallery_no_resolver_fn,
            dispatcher_live_fixture_with_external_products_url,
            dispatcher_test_conf_with_external_products_url_fn,
            dispatcher_test_conf_with_external_products_url,
            dispatcher_test_conf_with_matrix_options,
            dispatcher_test_conf_with_matrix_options_fn,
            dispatcher_live_fixture_with_matrix_options,
            dispatcher_test_conf_no_products_url,
            dispatcher_test_conf_no_products_url_fn,
            dispatcher_live_fixture_no_products_url,
            dispatcher_test_conf_fn,
            dispatcher_debug,
            sentry_sdk_fixture,
            dispatcher_nodebug,
            gunicorn_dispatcher,
            gunicorn_tmp_path,
            cleanup,
            empty_products_files_fixture,
            empty_products_user_files_fixture,
            default_params_dict,
            default_token_payload,
            dispatcher_live_fixture_empty_sentry,
            dispatcher_live_fixture_with_renku_options,
            dispatcher_test_conf_with_renku_options,
            dispatcher_test_conf_with_renku_options_fn
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
