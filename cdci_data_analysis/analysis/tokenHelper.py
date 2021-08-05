from types import FunctionType
import jwt

default_algorithm = 'HS256'


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


def get_decoded_token(token, secret_key):
    # decode the encoded token
    if token is not None:
        return jwt.decode(token, secret_key, algorithms=[default_algorithm])


def update_token_email_options(token, secret_key, new_options: dict):
    _valid_options_keys_list = ['msfail', 'msdone', 'mssub', 'intsub', 'mstout', 'tem']
    updated_token = token
    validation_dict = new_options.copy()

    # remove not needed keys
    for n in new_options.keys():
        if n not in _valid_options_keys_list:
            del validation_dict[n]

    def mutate_token_email_payload(token_payload):
        new_payload = token_payload.copy()
        new_payload.update(validation_dict)

        return new_payload

    # use the oda_api function
    # updated_token = oda_api.token.update_token(token, secret_key=secret_key, payload_mutation=mutate_token_email_payload)
    return updated_token



