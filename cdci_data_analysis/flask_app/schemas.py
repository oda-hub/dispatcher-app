import os

from marshmallow import Schema, EXCLUDE, fields
from marshmallow.validate import OneOf

dispatcher_strict_validate = os.environ.get('DISPATCHER_STRICT_VALIDATE', 'no') == 'yes'

class ExitStatus(Schema):
    class Meta:
        # TODO: adapt and remove this, so that only what is consumed by frontend is sent
        unknown = EXCLUDE

    status = fields.Int(validate=OneOf([0, 1]), required=dispatcher_strict_validate)
    message = fields.Str(description="if query_status == 'failed', shown in waitingDialog in red", required=dispatcher_strict_validate)
    error_message = fields.Str(description="if query_status == 'failed', shown in waitingDialog in red", required=dispatcher_strict_validate)
    debug_message = fields.Str(description="if query_status == 'done' but exit_status.status != 0, shown in waitingDialog in red", required=dispatcher_strict_validate)
    comment = fields.Str(description="always, shown in waitingDialog in yellow", required=dispatcher_strict_validate)
    warning = fields.Str(description="", required=dispatcher_strict_validate)


class QueryOutJSON(Schema):
    class Meta:
        unknown = EXCLUDE

    query_status = fields.Str(
                        validate=OneOf(["done", "failed", "submitted"]) if dispatcher_strict_validate else None,
                        description="",
                        required=dispatcher_strict_validate
                    )

    exit_status = fields.Nested(ExitStatus, required=True)
    session_id = fields.Str(required=False) # is it required?
    job_id = fields.Str(required=False) # is it required?

    error_message = fields.Str(
                        validate=OneOf([""]) if dispatcher_strict_validate else None,
                        description="",
                        required=False # but if present, should be empty
                    )

