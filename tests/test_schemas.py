from cdci_data_analysis.flask_app.schemas import ExitStatus
import pytest


def test_example_responses():
    from cdci_data_analysis.flask_app.schemas import QueryOutJSON

    #with pytest.raises(Exception):
    #    QueryOutJSON().load(dict())

    QueryOutJSON().load(
        dict(
            session_id="x",
            job_id="x",
            #query_status="done",
            exit_status=dict(
                debug_message="",
                status=1,
                message="",
                comment="",
                error_message="",
                warning="",
                arbitrary_unset_field="",
            ),
        )
    )
