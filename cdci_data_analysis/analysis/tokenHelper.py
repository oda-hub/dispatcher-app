import ast
import jwt
import oda_api.token
import logging

from cdci_data_analysis.analysis.exceptions import BadRequest

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


def update_token_email_options(token, secret_key, new_options):

    # TODO a dedicated configuration file is a better approach
    _valid_options_keys_types_dict = {
        'msfail': bool,
        'msdone': bool,
        'mssub': bool,
        'intsub': [int, float],
        'mstout': bool,
        'tem': [int, float],
    }
    _valid_options_keys_list = _valid_options_keys_types_dict.keys()
    validation_dict = new_options.copy()

    # remove not needed keys, and check types
    for n in new_options.keys():
        if n not in _valid_options_keys_list:
            del validation_dict[n]
        else:
            try:
                converted_value = ast.literal_eval(new_options[n])
            except Exception as e:
                raise BadRequest(f'The provided value of the option \'{n}\' cannot be properly interpreted, '
                                 'please check it and re-try to issue the request')

            if type(converted_value) == _valid_options_keys_types_dict[n] or \
                    type(converted_value) in _valid_options_keys_types_dict[n]:
                validation_dict[n] = converted_value
            else:
                printed_types = ','.join(map(lambda v: v.__name__, _valid_options_keys_types_dict[n]))
                raise BadRequest(f'The provided value of the option \'{n}\' is not of a valid type, '
                                 f'it should be one of the following: [{printed_types}]')

    def mutate_token_email_payload(token_payload):
        new_payload = token_payload.copy()
        new_payload.update(validation_dict)

        return new_payload

    # use the oda_api function
    updated_token = oda_api.token.update_token(token, secret_key=secret_key, payload_mutation=mutate_token_email_payload)
    return updated_token



