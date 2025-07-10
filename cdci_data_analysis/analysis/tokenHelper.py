import jwt
import os
import requests
import oda_api.token

from marshmallow import ValidationError
from typing import Tuple, Optional, Union

from cdci_data_analysis.analysis.exceptions import BadRequest
from cdci_data_analysis.flask_app.schemas import EmailOptionsTokenSchema
from cdci_data_analysis.app_logging import app_logging
from cdci_data_analysis.analysis.time_helper import validate_time

default_algorithm = 'HS256'
logger = app_logging.getLogger('tokenHelper')


def get_token_roles(decoded_token):
    # extract role(s)
    roles = None
    if 'roles' in decoded_token:
        if isinstance(decoded_token['roles'], str):
            roles = decoded_token['roles'].split(',')
        elif isinstance(decoded_token['roles'], list):
            roles = decoded_token['roles']
        roles = [r.strip() for r in roles]
    return roles


def set_token_roles(decoded_token, roles):
    decoded_token['roles'] = ','.join(roles)
    return decoded_token


def get_token_user(decoded_token):
    # extract user name
    return decoded_token['name'] if 'name' in decoded_token else ''


def get_roles_from_userinfo_claims(userinfo_claims):
    roles = []

    # claims_obj = {
    #     'developer': [],
    #     'maintainer': [],
    #     'owner': [],
    # }
    # TODO - this is a temporary solution, we should properly implement the logic to assign roles based on the userinfo claims
    if len(userinfo_claims['developer']) > 0 or len(userinfo_claims['maintainer']) > 0 or len(userinfo_claims['owner']) > 0:
        roles.append('oda workflow developer')

    if len(userinfo_claims['owner']) > 0:
        roles.append('administrator')
        roles.append('job manager')
        roles.append('gallery contributor')

    return roles


def get_openid_oauth_userinfo(oauth_host, access_token):
    userinfo_url = os.path.join(oauth_host, 'oauth/userinfo')
    headers = {
        'Authorization': 'Bearer ' + access_token,
    }
    userinfo_response = requests.get(userinfo_url, headers=headers)
    if userinfo_response.status_code == 200:
        return userinfo_response.json()
    else:
        logger.error(f"Failed to get userinfo: {userinfo_response.status_code} {userinfo_response.text}")
        return None


def get_gitlab_group_info(oauth_host, access_token, group_name):
    userinfo_url = os.path.join(oauth_host, f'api/v4/groups?search={group_name}')
    headers = {
        'Authorization': 'Bearer ' + access_token,
    }
    group_info_response = requests.get(userinfo_url, headers=headers)
    if group_info_response.status_code == 200:
        return group_info_response.json()
    else:
        logger.error(f"Failed to get group info: {group_info_response.status_code} {group_info_response.text}")
        return None


def get_gitlab_list_projects_groups(oauth_host, access_token, group_id):
    list_projects_url = os.path.join(oauth_host, f'api/v4/groups/{group_id}/projects')
    headers = {
        'Authorization': 'Bearer ' + access_token,
    }
    list_projects_response = requests.get(list_projects_url, headers=headers)
    if list_projects_response.status_code == 200:
        return list_projects_response.json()
    else:
        logger.error(f"Failed to get the list of projects of a given group: {list_projects_response.status_code} {list_projects_response.text}")
        return None


def get_userinfo_claims(userinfo):
    claims_obj = {
        'developer': [],
        'owner': [],
        'maintainer': [],
    }
    for i in userinfo:
        if i.endswith('claims/groups/developer'):
            claims_obj['developer'].extend(userinfo[i])
        elif i.endswith('claims/groups/owner'):
            claims_obj['owner'].extend(userinfo[i])
        elif i.endswith('claims/groups/maintainer'):
            claims_obj['maintainer'].extend(userinfo[i])

    return claims_obj


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


def encode_token(payload, secret_key, algorithm=default_algorithm):
    # encode the payload to a token
    if secret_key is None:
        raise BadRequest('A secret key must be provided to encode the token.')

    return jwt.encode(payload, secret_key, algorithm=algorithm)


def refresh_token(token, secret_key, refresh_interval):
    def refresh_token_exp_time(token_payload):

        refreshed_token_exp = token_payload['exp'] + refresh_interval

        try:
            validate_time(refreshed_token_exp)
        except (ValueError, OverflowError, TypeError, OSError) as e:
            logger.warning(
                f'Error when refreshing the token, the new value is invalid:\n{e}')
            # the range of values supported by the platform is commonly to be restricted to years in 1970 through 2038
            # but it might vary, this should accommodate the majority
            refreshed_token_exp = 2177449199

        refreshed_token_payload = {
            'exp': refreshed_token_exp
        }

        new_payload = token_payload.copy()
        new_payload.update(refreshed_token_payload)

        return new_payload

    # use the oda_api function
    updated_token = oda_api.token.update_token(token, secret_key=secret_key, payload_mutation=refresh_token_exp_time)
    return updated_token


def update_token_roles(token, secret_key, new_roles, allow_invalid=False):

    roles_dict = {'roles': new_roles}

    def mutate_token_roles(token_payload):
        new_payload = token_payload.copy()
        new_payload.update(roles_dict)

        return new_payload

    # use the oda_api function
    updated_token = oda_api.token.update_token(token, secret_key=secret_key, payload_mutation=mutate_token_roles, allow_invalid=allow_invalid)
    return updated_token


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


def validate_token_from_request(token, secret_key, required_roles=None, action="") -> Tuple[Union[str, dict, None], Optional[int]]:
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
            f"Unfortunately, your privileges are not sufficient to {action}.\n"
            f"Your privilege roles include {roles}, but the following roles are missing: {lacking_roles}."
        )
        return message, 403

    return decoded_token, None


def get_token_user_matrix_room_id(decoded_token):
    return decoded_token.get('mxroomid', None)


def get_token_user_submitted_matrix_message(decoded_token):
    return decoded_token.get('mxsub', None)


def get_token_user_sending_submitted_interval_matrix_message(decoded_token):
    return decoded_token.get('mxintsub', None)


def get_token_user_done_matrix_message(decoded_token):
    return decoded_token.get('mxdone', True)


def get_token_user_fail_matrix_message(decoded_token):
    return decoded_token.get('mxfail', True)


def get_token_user_timeout_threshold_matrix_message(decoded_token):
    return decoded_token.get('tmx', None)


def get_token_user_sending_timeout_matrix_message(decoded_token):
    return decoded_token.get('mxstout', None)

