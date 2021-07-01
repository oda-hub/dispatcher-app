from marshmallow import Schema, EXCLUDE, fields
from marshmallow.validate import OneOf

class ExitStatus(Schema):
    class Meta:
        unknown = EXCLUDE

    status = fields.Int(validate=OneOf([0, 1]), required=True)
    message = fields.Str(description="if query_status == 'failed', shown in waitingDialog in red", required=True)
    error_message = fields.Str(description="if query_status == 'failed', shown in waitingDialog in red", required=True)
    debug_message = fields.Str(description="if query_status == 'done' but exit_status.status != 0, shown in waitingDialog in red", required=True)
    comment = fields.Str(description="always, shown in waitingDialog in yellow", required=True)
    warning = fields.Str(description="", required=True)


class QueryOutJSON(Schema):
    class Meta:
        unknown = EXCLUDE

    query_status = fields.Str(
                        validate=OneOf(["done", "failed", "submitted"]),
                        description="",
                        required=True
                    )

    exit_status = fields.Nested(ExitStatus, required=True)
    session_id = fields.Str(required=True)
    job_id = fields.Str(required=True)

