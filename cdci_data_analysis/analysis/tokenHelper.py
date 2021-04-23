import jwt


def get_token_roles(decoded_token):
    # extract role(s)
    roles = decoded_token['roles'].split(',') if 'roles' in decoded_token else []
    roles[:] = [r.strip() for r in roles]
    return roles


def get_token_user(decoded_token):
    # extract user name
    return decoded_token['name'] if 'name' in decoded_token else ''


def get_token_user_email_address(decoded_token):
    # extract user email address
    return decoded_token['sub'] if 'sub' in decoded_token else ''


def get_token_user_timeout_threshold_mail(decoded_token):
    # extract user threshold
    return decoded_token['tem'] if 'tem' in decoded_token else ''


def get_token_user_sending_timeout_mail(decoded_token):
    # extract user threshold
    return decoded_token['mstout'] if 'mstout' in decoded_token else None


def get_token_user_submitted_email(decoded_token):
    # extract user threshold
    return decoded_token['mssub'] if 'mssub' in decoded_token else None


def get_decoded_token(token, secret_key):
    # decode the encoded token
    if token is not None:
        return jwt.decode(token, secret_key, algorithms=['HS256'])