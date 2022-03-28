import os
import json
import time

import jwt
from typing import Optional, Tuple, Dict

import requests
import base64
import copy
import uuid

from cdci_data_analysis.analysis import tokenHelper
from dateutil import parser
from enum import Enum, auto

from ..analysis.exceptions import RequestNotUnderstood, InternalError, RequestNotAuthorized
from ..flask_app.templates import body_article_product_gallery
from ..app_logging import app_logging

default_algorithm = 'HS256'

logger = app_logging.getLogger('drupal_helper')

n_max_tries = 10
retry_sleep_s = .5

total_n_successful_post_requests = 0
total_n_post_request_retries = 0


class ContentType(Enum):
    ARTICLE = auto()
    DATA_PRODUCT = auto()
    OBSERVATION = auto()
    ASTROPHYSICAL_ENTITY = auto()


def validate_token_gallery_request(token, secret_key):
    if token is None:
        return 'A token must be provided.', 403
    try:
        decoded_token = tokenHelper.get_decoded_token(token, secret_key)
        logger.info("==> token %s", decoded_token)
    except jwt.exceptions.ExpiredSignatureError:
        return 'The token provided is expired.', 403
    except jwt.exceptions.InvalidTokenError:
        return 'The token provided is not valid.', 403

    roles = tokenHelper.get_token_roles(decoded_token)

    required_roles = ['gallery contributor']
    if not all(item in roles for item in required_roles):
        lacking_roles = ", ".join(sorted(list(set(required_roles) - set(roles))))
        message = (
            f"Unfortunately, your privileges are not sufficient to post in the product gallery.\n"
            f"Your privilege roles include {roles}, but the following roles are missing: {lacking_roles}."
        )
        return message, 403

    return decoded_token, None


def analyze_drupal_output(drupal_output, operation_performed=None):
    if drupal_output.status_code < 200 or drupal_output.status_code >= 300:
        logger.warning(f'error while performing the following operation on the product gallery: {operation_performed}')
        logger.warning(f'the drupal instance returned the following error: {drupal_output.text}')
        raise RequestNotUnderstood(drupal_output.text,
                                   status_code=drupal_output.status_code,
                                   payload={'error_message': f'error while performing: {operation_performed}'})
    else:
        return drupal_output.json()


def get_list_terms(decoded_token, group, parent=None, disp_conf=None, sentry_client=None):
    gallery_secret_key = disp_conf.product_gallery_secret_key
    product_gallery_url = disp_conf.product_gallery_url
    # extract email address and then the relative user_id
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = get_user_id(product_gallery_url=product_gallery_url,
                                          user_email=user_email,
                                          sentry_client=sentry_client)
    # update the token
    gallery_jwt_token = generate_gallery_jwt_token(gallery_secret_key, user_id=user_id_product_creator)

    headers = get_drupal_request_headers(gallery_jwt_token)
    output_list = []
    output_request = None
    log_res = None

    if group is not None and str.lower(group) == 'instruments':
        if os.environ.get('DISPATCHER_DEBUG_MODE', 'no') == 'yes':
            parent = 'all'
        else:
            parent = 'production'
        log_res = execute_drupal_request(f"{product_gallery_url}/taxonomy/term_vocabulary_parent/instruments/{parent}?_format=hal_json",
                                         headers=headers)

    elif group is not None and str.lower(group) == 'products':
        if parent is None or parent == '':
            parent = 'all'
        log_res = execute_drupal_request(f"{product_gallery_url}/taxonomy/term_vocabulary_parent/products/{parent}?_format=hal_json",
                                         headers=headers)

    elif group is not None and str.lower(group) == 'sources':
        log_res = execute_drupal_request(f"{product_gallery_url}/astro_entities/source/all?_format=hal_json",
                                         headers=headers)

    if log_res is not None:
        output_request = analyze_drupal_output(log_res,
                                               operation_performed=f"retrieving the list of available {group} "
                                                                   "from the product gallery")

    if output_request is not None and type(output_request) == list and len(output_request) >= 0:
        for output in output_request:
            if 'name' in output:
                output_list.append(output['name'])
            elif 'title' in output:
                output_list.append(output['title'])

    return output_list


def get_parents_term(decoded_token, term, group=None, disp_conf=None, sentry_client=None):
    gallery_secret_key = disp_conf.product_gallery_secret_key
    product_gallery_url = disp_conf.product_gallery_url
    # extract email address and then the relative user_id from the mmoda token
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = get_user_id(product_gallery_url=product_gallery_url,
                                          user_email=user_email,
                                          sentry_client=sentry_client)
    # update the token
    gallery_jwt_token = generate_gallery_jwt_token(gallery_secret_key, user_id=user_id_product_creator)

    headers = get_drupal_request_headers(gallery_jwt_token)
    output_list = []
    output_request = None

    if group is None or group == '':
        group = 'all'
    log_res = execute_drupal_request(f"{product_gallery_url}/taxonomy/product_term_parent/{term}/{group}?_format=hal_json",
                                     headers=headers)

    if log_res is not None:
        msg = f"retrieving the list parents for the term {term}, "
        if group != '':
            msg += f"from the vocabulary {group}"
        output_request = analyze_drupal_output(log_res, operation_performed=(msg + ", from the product gallery"))

    if output_request is not None and type(output_request) == list and len(output_request) >= 0:
        for output in output_request:
            if 'parent_target_id' in output:
                output_list.append(output['parent_target_id'].split(','))

    return output_list


# TODO extend to support the sending of the requests also in other formats besides hal_json
# not necessary at the moment, but perhaps in the future it will be
def execute_drupal_request(url,
                           params=None,
                           data=None,
                           method='get',
                           headers=None,
                           files=None,
                           request_format='hal_json',
                           sentry_client=None):
    n_tries_left = n_max_tries
    global total_n_successful_post_requests, total_n_post_request_retries
    while True:
        try:
            if method == 'get':
                if params is None:
                    params = {}
                params['_format'] = request_format
                res = requests.get(url,
                                   params={**params},
                                   headers=headers)

            elif method == 'post':
                if data is None:
                    data = {}
                if params is None:
                    params = {}
                params['_format'] = request_format
                res = requests.post(url,
                                    params={**params},
                                    data=data,
                                    files=files,
                                    headers=headers
                                    )
            else:
                raise NotImplementedError
            if res.status_code == 403:
                try:
                    response_json = res.json()
                    # a 403 has been noticed to be returned in two different cases:
                    # * for not-valid token
                    # * not-completed request
                    error_msg = response_json['message']
                except json.decoder.JSONDecodeError:
                    error_msg = res.text
                raise RequestNotAuthorized(error_msg)

            elif res.status_code not in [200, 201]:
                logger.warning(f"there seems to be some problem in completing a request to the product gallery:\n"
                               f"the requested url {url} lead to the error {res.text}, "
                               "this might be due to an error in the url or the page requested no longer exists, "
                               "please check it and try to issue again the request")
                raise InternalError('issue when performing a request to the product gallery',
                                    status_code=500,
                                    payload={'error_message': res.text})
            else:
                total_n_successful_post_requests += 1

            return res

        except (ConnectionError,
                RequestNotAuthorized,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:

            n_tries_left -= 1
            total_n_post_request_retries += 1
            if total_n_successful_post_requests == 0:
                average_retries_request = 0
            else:
                average_retries_request = total_n_post_request_retries/total_n_successful_post_requests

            if n_tries_left > 0:
                if n_max_tries - n_tries_left > average_retries_request:
                    logger.warning(f"a request to the url {url} of the product gallery is taking more time than expected, "
                                   "we will investigate the problem and solve it as soon as possible")
                else:
                    logger.warning(f"there seems to be some problem in completing the request to the url {url} of the product gallery,"
                                   " this is possibly temporary and we will retry the same request shortly")

                logger.debug(f"{e} exception during a request to the url {url} of the product gallery\n"
                             f"{n_tries_left} tries left, sleeping {retry_sleep_s} seconds until retry\n"
                             f"average retries per request since dispatcher start: "
                             f"{average_retries_request:.2f}")
                time.sleep(retry_sleep_s)
            else:
                logger.warning(f"an issue occurred when performing a request to the product gallery, "
                               f"this prevented us to complete the request to the url: {url} \n"
                               f"this is likely to be a connection related problem, we are investigating and "
                               f"try to solve it as soon as possible")
                if sentry_client is not None:
                    sentry_client.capture('raven.events.Message',
                                          message=f'exception when performing a request to the product gallery: {repr(e)}')
                else:
                    logger.warning("sentry not used")
                raise InternalError('issue when performing a request to the product gallery',
                                    status_code=500,
                                    payload={'error_message': str(e)})


def get_drupal_request_headers(gallery_jwt_token=None):
    headers = {
        'Content-type': 'application/hal+json'
    }
    if gallery_jwt_token is not None:
        headers['Authorization'] = 'Bearer ' + gallery_jwt_token
    return headers


def generate_gallery_jwt_token(gallery_jwt_token_secret_key, user_id=None):
    iat = time.time()
    token_payload = dict(iat=iat,
                         exp=iat + 3600)
    if user_id is not None:
        drupal_obj = dict(
            uid=user_id
        )
        token_payload['drupal']=drupal_obj

    out_token = jwt.encode(token_payload, gallery_jwt_token_secret_key, algorithm=default_algorithm)

    return out_token


def get_user_id(product_gallery_url, user_email, sentry_client=None) -> Optional[str]:
    user_id = None
    headers = get_drupal_request_headers()

    # get the user id
    log_res = execute_drupal_request(f"{product_gallery_url}/users/{user_email}",
                                     headers=headers,
                                     sentry_client=sentry_client)
    output_get = analyze_drupal_output(log_res, operation_performed="retrieving the user id")
    if isinstance(output_get, list) and len(output_get) == 1:
        user_id = output_get[0]['uid']

    return user_id


def post_file_to_gallery(product_gallery_url, file, gallery_jwt_token, file_type="image", sentry_client=None):
    logger.info(f"uploading file {file} to the product gallery")

    body_post_file = copy.deepcopy(body_article_product_gallery.body_file)

    bytes_file = file.read()
    b_64_file = base64.b64encode(bytes_file).decode("utf8")
    file_name = file.filename
    img_extension = os.path.splitext(file_name)[1][1:]

    body_post_file["data"][0]["value"] = b_64_file
    body_post_file["uri"][0]["value"] = "public://" + file_name
    body_post_file["filename"][0]["value"] = file_name
    if file_type == "image":
        body_post_file["filemime"]["value"] = "image/" + img_extension
    else:
        body_post_file["filemime"]["value"] = file_type
    body_post_file["_links"]["type"]["href"] = os.path.join(product_gallery_url, body_post_file["_links"]["type"]["href"], file_type)

    headers = get_drupal_request_headers(gallery_jwt_token)

    # post the image
    log_res = execute_drupal_request(f"{product_gallery_url}/entity/file",
                                     method='post',
                                     data=json.dumps(body_post_file),
                                     headers=headers,
                                     sentry_client=sentry_client)
    logger.info(f"file {file} successfully uploaded to the product gallery")
    output_post = analyze_drupal_output(log_res, operation_performed="posting a picture to the product gallery")
    return output_post


def post_content_to_gallery(decoded_token,
                            files=None,
                            disp_conf=None,
                            **kwargs):

    gallery_secret_key = disp_conf.product_gallery_secret_key
    product_gallery_url = disp_conf.product_gallery_url

    sentry_url = getattr(disp_conf, 'sentry_url', None)
    sentry_client = None
    if sentry_url is not None:
        from raven import Client

        sentry_client = Client(sentry_url)

    par_dic = copy.deepcopy(kwargs)
    # extract email address and then the relative user_id
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = get_user_id(product_gallery_url=product_gallery_url,
                                          user_email=user_email,
                                          sentry_client=sentry_client)
    # update the token
    gallery_jwt_token = generate_gallery_jwt_token(gallery_secret_key, user_id=user_id_product_creator)

    par_dic['user_id_product_creator'] = user_id_product_creator
    # extract type of content to post
    content_type = ContentType[str.upper(par_dic.pop('content_type', 'article'))]
    fits_file_fid_list = None
    img_fid = None
    if content_type == content_type.DATA_PRODUCT:
        # process files sent
        if files is not None:
            for f in files:
                if f == 'img':
                    img_file_obj = files[f]
                    # upload file to drupal
                    output_img_post = post_file_to_gallery(product_gallery_url=product_gallery_url,
                                                           file_type="image",
                                                           file=img_file_obj,
                                                           gallery_jwt_token=gallery_jwt_token,
                                                           sentry_client=sentry_client)
                    img_fid = output_img_post['fid'][0]['value']
                elif f.startswith('fits_file'):
                    fits_file_obj = files[f]
                    # upload file to drupal
                    output_fits_file_post = post_file_to_gallery(product_gallery_url=product_gallery_url,
                                                                 file_type="document",
                                                                 file=fits_file_obj,
                                                                 gallery_jwt_token=gallery_jwt_token,
                                                                 sentry_client=sentry_client)
                    if fits_file_fid_list is None:
                        fits_file_fid_list = []
                    fits_file_fid_list.append(output_fits_file_post['fid'][0]['value'])

        session_id = par_dic.pop('session_id', None)
        job_id = par_dic.pop('job_id', None)
        product_title = par_dic.pop('product_title', None)
        observation_id = par_dic.pop('observation_id', None)
        user_id_product_creator = par_dic.pop('user_id_product_creator')
        # TODO perhaps there's a smarter way to do this
        insert_new_source = par_dic.pop('insert_new_source', 'False') == 'True'

        output_data_product_post = post_data_product_to_gallery(product_gallery_url=product_gallery_url,
                                                                session_id=session_id,
                                                                job_id=job_id,
                                                                gallery_jwt_token=gallery_jwt_token,
                                                                product_title=product_title,
                                                                img_fid=img_fid,
                                                                fits_file_fid_list=fits_file_fid_list,
                                                                observation_id=observation_id,
                                                                user_id_product_creator=user_id_product_creator,
                                                                insert_new_source=insert_new_source,
                                                                **par_dic)

        return output_data_product_post


def get_observations_for_time_range(product_gallery_url, gallery_jwt_token, t1=None, t2=None, sentry_client=None):
    observations = []
    # get from the drupal the relative id
    headers = get_drupal_request_headers(gallery_jwt_token)
    if t1 is None or t2 is None:
        formatted_range = 'all'
    else:
        # format the time fields, drupal does not provide (yet) the option to filter by date using also the time,
        # so the dates, properly formatted in ISO, without the time will be used
        t1_formatted = parser.parse(t1).strftime('%Y-%m-%d')
        t2formatted = parser.parse(t2).strftime('%Y-%m-%d')

    log_res = execute_drupal_request(f"{product_gallery_url}/observations/range_t1_t2/{t1_formatted}/{t2formatted}/",
                                     headers=headers,
                                     sentry_client=sentry_client)
    output_get = analyze_drupal_output(log_res, operation_performed="getting the observation range")
    if isinstance(output_get, list):
        observations = output_get

    return observations


def post_astro_entity(product_gallery_url, gallery_jwt_token, astro_entity_name, astro_entity_portal_link=None,  sentry_client=None):
    # post new observation with or without a specific time range
    body_gallery_astro_entity_node = copy.deepcopy(body_article_product_gallery.body_node)
    # set the type of content to post
    body_gallery_astro_entity_node["_links"]["type"]["href"] = os.path.join(product_gallery_url,
                                                                            body_gallery_astro_entity_node["_links"]["type"]["href"],
                                                                            'astro_entity')
    # TODO perhaps a bit of duplication here?
    body_gallery_astro_entity_node["title"]["value"] = astro_entity_name
    body_gallery_astro_entity_node["field_source_name"] = [{
            "value": astro_entity_name
        }]
    body_gallery_astro_entity_node["field_link"] = [{
        "value": astro_entity_portal_link
    }]

    headers = get_drupal_request_headers(gallery_jwt_token)

    log_res = execute_drupal_request(f"{product_gallery_url}/node",
                                     method='post',
                                     data=json.dumps(body_gallery_astro_entity_node),
                                     headers=headers,
                                     sentry_client=sentry_client)

    output_post = analyze_drupal_output(log_res, operation_performed="posting a new astrophysical entity")

    # extract the id of the observation
    astro_entity_drupal_id = output_post['nid'][0]['value']

    return astro_entity_drupal_id


def post_observation(product_gallery_url, gallery_jwt_token, t1=None, t2=None, sentry_client=None):
    # post new observation with or without a specific time range
    body_gallery_observation_node = copy.deepcopy(body_article_product_gallery.body_node)
    # set the type of content to post
    body_gallery_observation_node["_links"]["type"]["href"] = os.path.join(product_gallery_url, body_gallery_observation_node["_links"]["type"][
                                                                  "href"], 'observation')
    if t1 is not None and t2 is not None:
        # format the time fields, from the format request
        t1_formatted = parser.parse(t1).strftime('%Y-%m-%dT%H:%M:%S')
        t2_formatted = parser.parse(t2).strftime('%Y-%m-%dT%H:%M:%S')
        # set the daterange
        body_gallery_observation_node["field_timerange"] = [{
            "value": t1_formatted,
            "end_value": t2_formatted
        }]

        body_gallery_observation_node["title"]["value"] = "_".join(["observation", t1_formatted, t2_formatted])
    else:
        # assign a randomly generate id in case to time range is provided
        body_gallery_observation_node["title"]["value"] = "_".join(["observation", str(uuid.uuid4())])

    headers = get_drupal_request_headers(gallery_jwt_token)

    log_res = execute_drupal_request(f"{product_gallery_url}/node",
                                     method='post',
                                     data=json.dumps(body_gallery_observation_node),
                                     headers=headers,
                                     sentry_client=sentry_client)

    output_post = analyze_drupal_output(log_res, operation_performed="posting a new observation")

    # extract the id of the observation
    observation_drupal_id = output_post['nid'][0]['value']

    return observation_drupal_id


# TODO to further optimize in two separate calls
def get_instrument_product_type_id(product_gallery_url, gallery_jwt_token, product_type=None, instrument=None, sentry_client=None) \
        -> Dict:
    output_dict = {}

    headers = get_drupal_request_headers(gallery_jwt_token)
    if product_type is not None or instrument is not None:
        # TODO improve this REST endpoint on drupal to accept multiple input terms, and give one result per input
        # get all the taxonomy terms
        log_res = execute_drupal_request(f"{product_gallery_url}/taxonomy/term_name/all?_format=hal_json",
                                         headers=headers,
                                         sentry_client=sentry_client)
        output_post = analyze_drupal_output(log_res,
                                            operation_performed="retrieving the taxonomy terms from the product gallery")
        if type(output_post) == list and len(output_post) > 0:
            for output in output_post:
                if instrument is not None and output['vid'] == 'Instruments' and output['name'] == instrument:
                    # info for the instrument
                    output_dict['instrument_id'] = int(output['tid'])
                if product_type is not None and output['vid'] == 'product_type' and output['name'] == product_type:
                    # info for the product
                    output_dict['product_type_id'] = int(output['tid'])

    return output_dict


def get_source_astrophysical_entity_id_by_source_name(product_gallery_url, gallery_jwt_token, source_name=None, sentry_client=None) \
        -> Optional[str]:
    entities_id = None
    # get from the drupal the relative id
    headers = get_drupal_request_headers(gallery_jwt_token)

    log_res = execute_drupal_request(f"{product_gallery_url}/astro_entities/source/{source_name}",
                                     headers=headers,
                                     sentry_client=sentry_client)
    output_get = analyze_drupal_output(log_res, operation_performed="retrieving the astrophysical entity information")
    if isinstance(output_get, list) and len(output_get) == 1:
        entities_id = output_get[0]['nid']

    return entities_id


def get_observation_drupal_id(product_gallery_url, gallery_jwt_token,
                              t1=None, t2=None,
                              observation_id=None,
                              sentry_client=None) \
        -> Tuple[Optional[str], Optional[str]]:
    observation_drupal_id = None
    observation_information_message = None
    if observation_id is not None:
        # get from the drupal the relative id
        headers = get_drupal_request_headers(gallery_jwt_token)

        log_res = execute_drupal_request(f"{product_gallery_url}/observations/{observation_id}",
                                         headers=headers,
                                         sentry_client=sentry_client)
        output_get = analyze_drupal_output(log_res, operation_performed="retrieving the observation information")

        if isinstance(output_get, list) and len(output_get) == 1:
            observation_drupal_id = output_get[0]['nid']
            observation_information_message = 'observation assigned by the user'
    else:

        if t1 is not None and t2 is not None:
            observations_range = get_observations_for_time_range(product_gallery_url, gallery_jwt_token, t1=t1, t2=t2, sentry_client=sentry_client)
            for observation in observations_range:
                times = observation['field_timerange'].split(' - ')
                parsed_t1 = parser.parse(t1)
                parsed_t2 = parser.parse(t2)
                t_start = parser.parse(times[0])
                t_end = parser.parse(times[1])
                if t_start == parsed_t1 and t_end == parsed_t2:
                    observation_drupal_id = observation['nid']
                    observation_information_message = 'observation assigned from the provided time range'
                    break

        if observation_drupal_id is None and (t1 is not None and t2 is not None):
            observation_drupal_id = post_observation(product_gallery_url, gallery_jwt_token, t1, t2, sentry_client=sentry_client)
            observation_information_message = 'a new observation has been posted'

    return observation_drupal_id, observation_information_message


def post_data_product_to_gallery(product_gallery_url, gallery_jwt_token,
                                 session_id=None,
                                 job_id=None,
                                 product_title=None,
                                 img_fid=None,
                                 fits_file_fid_list=None,
                                 observation_id=None,
                                 user_id_product_creator=None,
                                 insert_new_source=False,
                                 sentry_client=None,
                                 **kwargs):
    body_gallery_article_node = copy.deepcopy(body_article_product_gallery.body_node)

    # set the type of content to post
    body_gallery_article_node["_links"]["type"]["href"] = os.path.join(product_gallery_url, body_gallery_article_node["_links"]["type"][
                                                                  "href"], 'data_product')

    # set the initial body content
    body_value = ''
    t1 = t2 = instrument = product_type = None
    if session_id is not None and job_id is not None:

        # in case job_id and session_id are passed then it automatically extracts the product information
        # related to the specific job, otherwise what will be posted will hav to entirely provided by the user

        scratch_dir = f'scratch_sid_{session_id}_jid_{job_id}'
        # the aliased version might have been created
        scratch_dir_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased'
        analysis_parameters_json_content_original = None
        #
        if os.path.exists(scratch_dir):
            analysis_parameters_json_content_original = json.load(open(scratch_dir + '/analysis_parameters.json'))
        elif os.path.exists(scratch_dir_json_fn_aliased):
            analysis_parameters_json_content_original = json.load(
                open(scratch_dir_json_fn_aliased + '/analysis_parameters.json'))

        if analysis_parameters_json_content_original is not None:
            analysis_parameters_json_content_original.pop('token', None)
            instrument = analysis_parameters_json_content_original.pop('instrument')
            product_type = analysis_parameters_json_content_original.pop('product_type')
            # time data for the observation
            t1 = analysis_parameters_json_content_original.pop('T1')
            t2 = analysis_parameters_json_content_original.pop('T2')

            # TODO no need to set all the parameters by default
            # for k, v in analysis_parameters_json_content_original.items():
            #     # assuming the name of the field in drupal starts always with field_
            #     field_name = str.lower('field_' + k)
            #     body_gallery_article_node[field_name] = [{
            #         "value": v
            #     }]
            body_value = ''
        else:
            raise RequestNotUnderstood(message="Request data not found",
                                       payload={'error_message': 'error while posting data product: '
                                                                 'results of the ODA product request could not be found, '
                                                                 'perhaps wrong job_id was passed?'})

    # extract user-provided instrument and product_type
    if 'instrument' in kwargs:
        instrument = kwargs.pop('instrument')
    if 'product_type' in kwargs:
        product_type = kwargs.pop('product_type')

    # set observation
    if 'T1' in kwargs:
        t1 = kwargs.pop('T1')
    if 'T2' in kwargs:
        t2 = kwargs.pop('T2')

    observation_drupal_id, observation_information_message = get_observation_drupal_id(product_gallery_url, gallery_jwt_token,
                                                      t1=t1, t2=t2, observation_id=observation_id)
    if observation_drupal_id is not None:
        body_gallery_article_node["field_derived_from_observation"] = [{
            "target_id": observation_drupal_id
        }]

    if observation_information_message is not None:
        logger.info("==> information about assigned observation: %s", observation_information_message)

    body_gallery_article_node["body"][0]["value"] = body_value

    # set the user id of the author of the data product
    if user_id_product_creator is not None:
        body_gallery_article_node["uid"] = [{
            "target_id": user_id_product_creator
        }]

    src_name = kwargs.pop('src_name', None)
    src_portal_link = kwargs.pop('entity_portal_link', None)
    # set the source astrophysical entity if available
    if src_name is not None:
        source_entity_id = get_source_astrophysical_entity_id_by_source_name(product_gallery_url, gallery_jwt_token,
                                                                             source_name=src_name,
                                                                             sentry_client=sentry_client)
        # create a new source ? yes if the user wants it
        if source_entity_id is None and insert_new_source:
            source_entity_id = post_astro_entity(product_gallery_url, gallery_jwt_token,
                                                 astro_entity_name=src_name,
                                                 astro_entity_portal_link=src_portal_link,
                                                 sentry_client=sentry_client)

        if source_entity_id is not None:
            body_gallery_article_node['field_describes_astro_entity'] = [{
                "target_id": int(source_entity_id)
            }]

    # set the product title
    # TODO agree on a better logic to assign the product title
    if product_title is None:
        if product_type is None and src_name is None:
            product_title = "_".join(["data_product", str(uuid.uuid4())])
        elif product_type is None and src_name is not None:
            product_title = src_name
        elif product_type is not None and src_name is None:
            product_title = product_type
        else:
            product_title = "_".join([src_name, product_type])

    body_gallery_article_node["title"]["value"] = product_title

    ids_obj = get_instrument_product_type_id(product_gallery_url=product_gallery_url,
                                             gallery_jwt_token=gallery_jwt_token,
                                             product_type=product_type,
                                             instrument=instrument)
    if 'instrument_id' in ids_obj:
        # info for the instrument
        body_gallery_article_node['field_instrumentused'] = [{
            "target_id": ids_obj['instrument_id']
        }]

    if 'product_type_id' in ids_obj:
        # info for the product
        body_gallery_article_node['field_data_product_type'] = [{
            "target_id": ids_obj['product_type_id']
        }]

    # let's go through the kwargs and if any overwrite some values for the product to post
    for k, v in kwargs.items():
        # assuming the name of the field in drupal starts always with field_
        field_name = str.lower('field_' + k)
        body_gallery_article_node[field_name] = [{
            "value": v
        }]

    # setting img fid if available
    if img_fid is not None:
        body_gallery_article_node['field_image_png'] = [{
            "target_id": int(img_fid)
        }]
    # setting fits file fid if available
    if fits_file_fid_list is not None:
        for fid in fits_file_fid_list:
            if 'field_fits_file' not in body_gallery_article_node:
                body_gallery_article_node['field_fits_file'] = []
            body_gallery_article_node['field_fits_file'].append({
                "target_id": int(fid)
            })
    # finally, post the data product to the gallery
    headers = get_drupal_request_headers(gallery_jwt_token)
    log_res = execute_drupal_request(f"{product_gallery_url}/node",
                                     method='post',
                                     data=json.dumps(body_gallery_article_node),
                                     headers=headers,
                                     sentry_client=sentry_client)

    output_post = analyze_drupal_output(log_res, operation_performed="posting data product to the gallery")

    return output_post


def resolve_name(name_resolver_url: str, entities_portal_url: str = None, name: str = None):
    resolved_obj = {}
    if name is not None:
        res = requests.get(name_resolver_url.format(name))
        if res.status_code == 200:
            returned_resolved_obj = res.json()
            if 'success' in returned_resolved_obj:
                resolved_obj['name'] = name.replace('_', ' ')
                if returned_resolved_obj['success']:
                    logger.info(f"object {name} successfully resolved")
                    if 'ra' in returned_resolved_obj:
                        resolved_obj['RA'] = float(returned_resolved_obj['ra'])
                    if 'dec' in returned_resolved_obj:
                        resolved_obj['DEC'] = float(returned_resolved_obj['ra'])
                    resolved_obj['entity_portal_link'] = entities_portal_url.format(name)
                    resolved_obj['message'] = f'{name} successfully resolved'
                elif not returned_resolved_obj['success']:
                    logger.info(f"resolution of the object {name} unsuccessful")
                    resolved_obj['message'] = f'{name} could not be resolved'
        else:
            logger.warning(f"there seems to be some problem in completing the request for the resolution of the object: {name}\n"
                           f"the request lead to the error {res.text}, "
                           "this might be due to an error in the url or the service "
                           "requested is currently not available, "
                           "please check your request and try to issue it again")
            raise InternalError('issue when performing a request to the local resolver',
                                status_code=500,
                                payload={'error_message': res.text})
    return resolved_obj
