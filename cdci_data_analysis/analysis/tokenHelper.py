import jwt
import oda_api.token
from marshmallow import ValidationError

from cdci_data_analysis.analysis.exceptions import BadRequest
from cdci_data_analysis.flask_app.schemas import EmailOptionsTokenSchema
from cdci_data_analysis.app_logging import app_logging

default_algorithm = 'HS256'
logger = app_logging.getLogger('tokenHelper')


def get_token_roles(decoded_token):
    # extract role(s)
    if isinstance(decoded_token['roles'], str):
        roles = decoded_token['roles'].split(',') if 'roles' in decoded_token else []
        roles[:] = [r.strip() for r in roles]
    elif isinstance(decoded_token['roles'], list):
        roles = decoded_token['roles'] if 'roles' in decoded_token else []
        roles[:] = [r.strip() for r in roles]
    return roles


def get_token_user(decoded_token):
    # extract user name
    return decoded_token['name'] if 'name' in decoded_token else ''


def get_token_user_email_address(decoded_token):
    # extract user email address
    if 'sub' in decoded_token:
        return decoded_token['sub']
    if 'email' in decoded_token:
        return decoded_token['email']
    return ''


def get_token_user_timeout_threshold_email(decoded_token):
    # extract user threshold
    return decoded_token['tem'] if 'tem' in decoded_token else None


def get_token_user_sending_timeout_email(decoded_token):
    return decoded_token['mstout'] if 'mstout' in decoded_token else None


def get_token_user_sending_submitted_interval_email(decoded_token):
    return decoded_token['intsub'] if 'intsub' in decoded_token else None


def get_token_user_submitted_email(decoded_token):
    return decoded_token['mssub'] if 'mssub' in decoded_token else None


def get_token_user_done_email(decoded_token):
    return decoded_token.get('msdone', True) # TODO: make server configurable


def get_token_user_fail_email(decoded_token):
    return decoded_token.get('msfail', True) # TODO: make server configurable


def get_decoded_token(token, secret_key, validate_token=True):
    # decode the encoded token
    if token is not None:
        if validate_token:
            return jwt.decode(token, secret_key, algorithms=[default_algorithm])
        else:
            return jwt.decode(token, "",
                              algorithms=[default_algorithm],
                              options=dict(
                                verify_signature=False
                            ))


def update_token_email_options(token, secret_key, new_options):

    validation_dict = {}
    try:
        validation_dict = EmailOptionsTokenSchema().load(new_options)
    except ValidationError as e:
        raise BadRequest(f'An error occurred while validating the following fields: {e.messages}. '
                         f'Please check it and re-try to issue the request')

    def mutate_token_email_payload(token_payload):
        new_payload = token_payload.copy()
        new_payload.update(validation_dict)

        return new_payload

    # use the oda_api function
    updated_token = oda_api.token.update_token(token, secret_key=secret_key, payload_mutation=mutate_token_email_payload)
    return updated_token


def validate_token_from_request(token, secret_key, required_roles=None):
    if token is None:
        return 'A token must be provided.', 403
    try:
        decoded_token = get_decoded_token(token, secret_key)
        logger.info("==> token %s", decoded_token)
    except jwt.exceptions.ExpiredSignatureError:
        return 'The token provided is expired.', 403
    except jwt.exceptions.InvalidTokenError:
        return 'The token provided is not valid.', 403

    roles = get_token_roles(decoded_token)

    if required_roles is None:
        required_roles = []

    if not all(item in roles for item in required_roles):
        lacking_roles = ", ".join(sorted(list(set(required_roles) - set(roles))))
        message = (
            f"Unfortunately, your privileges are not sufficient to post in the product gallery.\n"
            f"Your privilege roles include {roles}, but the following roles are missing: {lacking_roles}."
        )
        return message, 403

    return decoded_token, None
