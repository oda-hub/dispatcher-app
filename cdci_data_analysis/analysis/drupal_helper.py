import os
import json
import time
import urllib.parse

import jwt
import numpy as np
import requests
import base64
import copy
import uuid
import glob
import re

from typing import Optional, Tuple, Dict

from ..flask_app.sentry import sentry

from dateutil import parser, tz
from datetime import datetime
from enum import Enum, auto
from astropy.coordinates import SkyCoord, Angle
from astropy import units as u

from cdci_data_analysis.analysis import tokenHelper
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
    REVOLUTION_PROCESSING_LOG = auto()


def analyze_drupal_output(drupal_output, operation_performed=None):
    if drupal_output.status_code < 200 or drupal_output.status_code >= 300:
        logger.warning(f'error while performing the following operation on the product gallery: {operation_performed}')
        logger.warning(f'the drupal instance returned the following error: {drupal_output.text}')
        raise RequestNotUnderstood(drupal_output.text,
                                   status_code=drupal_output.status_code,
                                   payload={'drupal_helper_error_message': f'error while performing: {operation_performed}'})
    else:
        if drupal_output.headers.get('content-type') == 'application/hal+json':
            return drupal_output.json()
        return drupal_output.text


def get_list_terms(decoded_token, group, parent=None, disp_conf=None, sentry_dsn=None):
    gallery_secret_key = disp_conf.product_gallery_secret_key
    product_gallery_url = disp_conf.product_gallery_url
    # extract email address and then the relative user_id
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = get_user_id(product_gallery_url=product_gallery_url,
                                          user_email=user_email,
                                          sentry_dsn=sentry_dsn)
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
                                         headers=headers, sentry_dsn=sentry_dsn)

    elif group is not None and str.lower(group) == 'products':
        if parent is None or parent == '':
            parent = 'all'
        log_res = execute_drupal_request(f"{product_gallery_url}/taxonomy/term_vocabulary_parent/products/{parent}?_format=hal_json",
                                         headers=headers, sentry_dsn=sentry_dsn)

    elif group is not None and str.lower(group) == 'sources':
        log_res = execute_drupal_request(f"{product_gallery_url}/astro_entities/source/all?_format=hal_json",
                                         headers=headers, sentry_dsn=sentry_dsn)

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


def get_parents_term(decoded_token, term, group=None, disp_conf=None, sentry_dsn=None):
    gallery_secret_key = disp_conf.product_gallery_secret_key
    product_gallery_url = disp_conf.product_gallery_url
    # extract email address and then the relative user_id from the mmoda token
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = get_user_id(product_gallery_url=product_gallery_url,
                                          user_email=user_email,
                                          sentry_dsn=sentry_dsn)
    # update the token
    gallery_jwt_token = generate_gallery_jwt_token(gallery_secret_key, user_id=user_id_product_creator)

    headers = get_drupal_request_headers(gallery_jwt_token)
    output_list = []
    output_request = None

    if group is None or group == '':
        group = 'all'
    log_res = execute_drupal_request(f"{product_gallery_url}/taxonomy/product_term_parent/{term}/{group}?_format=hal_json",
                                     headers=headers, sentry_dsn=sentry_dsn)

    if log_res is not None:
        msg = f"retrieving the list parents for the term {term}, "
        if group != '':
            msg += f"from the vocabulary {group}"
        output_request = analyze_drupal_output(log_res, operation_performed=(msg + ", from the product gallery"))

    if output_request is not None and type(output_request) == list and len(output_request) >= 0:
        for output in output_request:
            if 'parent_target_id' in output:
                parents_list = output['parent_target_id'].split(',')
                output_list.extend([r.strip() for r in parents_list])

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
                           sentry_dsn=None):
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
            elif method == 'patch':
                if data is None:
                    data = {}
                if params is None:
                    params = {}
                params['_format'] = request_format
                res = requests.patch(url,
                                     params={**params},
                                     data=data,
                                     files=files,
                                     headers=headers
                                     )
            elif method == 'delete':
                if data is None:
                    data = {}
                if params is None:
                    params = {}
                params['_format'] = request_format
                res = requests.delete(url,
                                      params={**params},
                                      data=data,
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

            elif res.status_code not in [200, 201, 204]:
                logger.warning(f"there seems to be some problem in completing a request to the product gallery:\n"
                               f"the requested url {url} lead to the error {res.text}, "
                               "this might be due to an error in the url or the page requested no longer exists, "
                               "please check it and try to issue again the request")
                drupal_helper_error_message = res.text
                # handling specific case of a not recognized/invalid argument
                m = re.search(r'<em(.*)>InvalidArgumentException</em>:(.*)</em>\)', res.text)
                if m is not None:
                    drupal_helper_error_message = re.sub('<[^<]+?>', '', m.group())

                if sentry_dsn is not None:
                    sentry.capture_message(f'issue in completing a request to the product gallery: '
                                           f'the requested url {url} lead to the error '
                                           f'{drupal_helper_error_message}')
                else:
                    logger.warning("sentry not used")

                raise InternalError('issue when performing a request to the product gallery',
                                    status_code=500,
                                    payload={'drupal_helper_error_message': drupal_helper_error_message})
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
                sentry.capture_message(f'exception when performing a request to the product gallery: {repr(e)}')                
                raise InternalError('issue when performing a request to the product gallery',
                                    status_code=500,
                                    payload={'drupal_helper_error_message': str(e)})


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


def get_user_id(product_gallery_url, user_email, sentry_dsn=None) -> Optional[str]:
    user_id = None
    headers = get_drupal_request_headers()

    # get the user id
    log_res = execute_drupal_request(f"{product_gallery_url}/users/{user_email}",
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)
    output_get = analyze_drupal_output(log_res, operation_performed="retrieving the user id")
    if isinstance(output_get, list) and len(output_get) == 1:
        user_id = output_get[0]['uid']

    return user_id


def delete_file_gallery(product_gallery_url, file_to_delete_id, gallery_jwt_token, sentry_dsn=None):
    logger.info(f"deleting file with id {file_to_delete_id} from the product gallery")

    headers = get_drupal_request_headers(gallery_jwt_token)

    log_res = execute_drupal_request(f"{product_gallery_url}/file/{file_to_delete_id}",
                                     method='delete',
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)

    logger.info(f"file with {file_to_delete_id} successfully deleted from the product gallery")
    output_post = analyze_drupal_output(log_res, operation_performed="deleting a file from the product gallery")
    return output_post


def post_file_to_gallery(product_gallery_url, file, gallery_jwt_token, file_type="image", sentry_dsn=None):
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

    log_res = execute_drupal_request(f"{product_gallery_url}/entity/file",
                                     method='post',
                                     data=json.dumps(body_post_file),
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)

    logger.info(f"file {file} successfully uploaded to the product gallery")
    output_post = analyze_drupal_output(log_res, operation_performed="posting a picture to the product gallery")
    return output_post


def post_content_to_gallery(decoded_token,
                            files=None,
                            disp_conf=None,
                            **kwargs):

    gallery_secret_key = disp_conf.product_gallery_secret_key
    product_gallery_url = disp_conf.product_gallery_url
    converttime_revnum_service_url = disp_conf.converttime_revnum_service_url
    timezone = disp_conf.product_gallery_timezone

    sentry_dsn = sentry.sentry_url

    par_dic = copy.deepcopy(kwargs)
    # extract email address and then the relative user_id
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = get_user_id(product_gallery_url=product_gallery_url,
                                          user_email=user_email,
                                          sentry_dsn=sentry_dsn)
    # update the token
    gallery_jwt_token = generate_gallery_jwt_token(gallery_secret_key, user_id=user_id_product_creator)

    par_dic['user_id_product_creator'] = user_id_product_creator

    # extract type of content to post
    content_type = ContentType[str.upper(par_dic.pop('content_type', 'article'))]
    fits_file_fid_list = None
    html_file_fid_list = None
    yaml_file_fid_list = None
    img_fid = None
    product_title = None
    data_product_id = None
    output_content_post = None
    if files is not None:
        for f in files:
            if f == 'img':
                img_file_obj = files[f]
                # upload file to drupal
                output_img_post = post_file_to_gallery(product_gallery_url=product_gallery_url,
                                                       file_type="image",
                                                       file=img_file_obj,
                                                       gallery_jwt_token=gallery_jwt_token,
                                                       sentry_dsn=sentry_dsn)
                img_fid = output_img_post['fid'][0]['value']
            else:
                output_file_post = post_file_to_gallery(product_gallery_url=product_gallery_url,
                                                        file_type="document",
                                                        file=files[f],
                                                        gallery_jwt_token=gallery_jwt_token,
                                                        sentry_dsn=sentry_dsn)
                if f.startswith('fits_file'):
                    if fits_file_fid_list is None:
                        fits_file_fid_list = []
                    fits_file_fid_list.append(output_file_post['fid'][0]['value'])
                elif f.startswith('html_file'):
                    if html_file_fid_list is None:
                        html_file_fid_list = []
                    html_file_fid_list.append(output_file_post['fid'][0]['value'])
                elif f.startswith('yaml_file'):
                    if yaml_file_fid_list is None:
                        yaml_file_fid_list = []
                    yaml_file_fid_list.append(output_file_post['fid'][0]['value'])
    if content_type == content_type.DATA_PRODUCT:

        product_id = par_dic.get('product_id', None)
        if product_id is not None:
            logger.info(f"retrieving data-products with the provided product_id: {product_id}")
            product_id_data_product_list = get_data_product_list_by_product_id(product_gallery_url=product_gallery_url,
                                                                               gallery_jwt_token=gallery_jwt_token,
                                                                               product_id=product_id,
                                                                               sentry_dsn=sentry_dsn)
            if len(product_id_data_product_list) > 0:
                if len(product_id_data_product_list) > 1:
                    logger.info(f"more than one data-product with product_id {product_id} has been found, the most recently posted or updated will be used")
                data_product_id = product_id_data_product_list[0]['nid']
                product_title = product_id_data_product_list[0]['title']
                logger.info(f"the data-product \"{product_title}\", id: {data_product_id} will be updated")

        # process files sent

        product_title = par_dic.pop('product_title', product_title)
        observation_id = par_dic.pop('observation_id', None)
        user_id_product_creator = par_dic.pop('user_id_product_creator')
        # TODO perhaps there's a smarter way to do this
        insert_new_source = par_dic.pop('insert_new_source', 'False') == 'True'

        output_content_post = post_data_product_to_gallery(product_gallery_url=product_gallery_url,
                                                           gallery_jwt_token=gallery_jwt_token,
                                                           converttime_revnum_service_url=converttime_revnum_service_url,
                                                           data_product_id=data_product_id,
                                                           product_title=product_title,
                                                           img_fid=img_fid,
                                                           fits_file_fid_list=fits_file_fid_list,
                                                           html_file_fid_list=html_file_fid_list,
                                                           observation_attachment_file_fid_list=yaml_file_fid_list,
                                                           observation_id=observation_id,
                                                           user_id_product_creator=user_id_product_creator,
                                                           insert_new_source=insert_new_source,
                                                           timezone=timezone,
                                                           sentry_dsn=sentry_dsn,
                                                           **par_dic)
    elif content_type == content_type.REVOLUTION_PROCESSING_LOG:
        user_id_product_creator = par_dic.pop('user_id_product_creator')
        output_content_post = post_revolution_processing_log_to_gallery(product_gallery_url=product_gallery_url,
                                                                        gallery_jwt_token=gallery_jwt_token,
                                                                        user_id_product_creator=user_id_product_creator,
                                                                        sentry_dsn=sentry_dsn,
                                                                        **par_dic)
    elif content_type == content_type.OBSERVATION:
        # TODO build the body to send to the gallery in more automated fashion (like done for the data-product)
        t1 = kwargs.pop('T1', None)
        t2 = kwargs.pop('T2', None)
        revnum_1 = kwargs.pop('revnum_1', None)
        revnum_2 = kwargs.pop('revnum_2', None)
        obsid = kwargs.pop('obsid', None)
        title = kwargs.pop('title', None)
        update_observation = kwargs.pop('update_observation', 'False') == 'True'
        create_new = kwargs.pop('create_new', 'False') == 'True'
        output_content_post = post_observation(product_gallery_url=product_gallery_url,
                                               gallery_jwt_token=gallery_jwt_token,
                                               converttime_revnum_service_url=converttime_revnum_service_url,
                                               title=title,
                                               t1=t1, t2=t2, timezone=timezone,
                                               revnum_1=revnum_1, revnum_2=revnum_2,
                                               obsids=obsid,
                                               observation_attachment_file_fid_list=yaml_file_fid_list,
                                               sentry_dsn=sentry_dsn,
                                               update_observation=update_observation,
                                               create_new=create_new)
        if output_content_post is not None:
            # extract the id of the observation
            observation_drupal_id = output_content_post['nid'][0]['value']
            logger.info(f"observation with id {observation_drupal_id} has been successfully posted")
        else:
            output_content_post = {}
            logger.info(f"no observation has been created or updated")

    elif content_type == content_type.ASTROPHYSICAL_ENTITY:
        # TODO build the body to send to the gallery in more automated fashion (like done for the data-product)
        update_astro_entity = kwargs.pop('update_astro_entity', 'False') == 'True'
        src_name = kwargs.pop('src_name', None)
        source_entity_id = None
        source_ra = None
        source_dec = None
        src_portal_link = None
        object_type = None
        object_ids = None
        if update_astro_entity:
            auto_update = kwargs.pop('auto_update', 'False') == 'True'
            if auto_update is True:
                name_resolver_url = disp_conf.name_resolver_url
                entities_portal_url = disp_conf.entities_portal_url
                resolved_obj = resolve_name(name_resolver_url=name_resolver_url,
                                            entities_portal_url=entities_portal_url,
                                            name=src_name)
                if resolved_obj is not None:
                    msg = ''
                    if 'message' in resolved_obj:
                        if 'could not be resolved' in resolved_obj['message']:
                            msg = f'\nSource {src_name} could not be validated'
                        elif 'successfully resolved' in resolved_obj['message']:
                            msg = f'\nSource {src_name} was successfully validated'
                    msg += '\n'
                    logger.info(msg)
                    if 'RA' in resolved_obj:
                        source_ra = Angle(resolved_obj["RA"], unit='degree').deg
                    if 'DEC' in resolved_obj:
                        source_dec = Angle(resolved_obj["DEC"], unit='degree').deg
                    if 'entity_portal_link' in resolved_obj:
                        src_portal_link = resolved_obj['entity_portal_link']
                    if 'object_type' in resolved_obj:
                        object_type = resolved_obj['object_type']
                    if 'object_ids' in resolved_obj:
                        object_ids = resolved_obj['object_ids']
            else:
                src_portal_link = kwargs.pop('src_portal_link', None)
                source_ra = kwargs.pop('source_ra', None)
                source_dec = kwargs.pop('source_dec', None)
                object_type = kwargs.pop('object_type', None)
                object_ids = kwargs.pop('object_ids', None)

            source_entity_id = get_source_astrophysical_entity_id_by_source_name(product_gallery_url,
                                                                                 gallery_jwt_token,
                                                                                 source_name=src_name,
                                                                                 sentry_dsn=sentry_dsn)
        if update_astro_entity and source_entity_id is None:
            logger.warning(f'an update of an astrophysical entity could not be performed since the correspondent one '
                           f'could not be found, please check the provided name')
            raise RequestNotUnderstood(message="Request data not found",
                                       payload={'drupal_helper_error_message': 'error while updating astrophysical and '
                                                                               'entity product: no correspondent entity '
                                                                               'could be found with the provided name'})

        output_content_post = post_astro_entity(product_gallery_url=product_gallery_url,
                                                gallery_jwt_token=gallery_jwt_token,
                                                astro_entity_name=src_name.strip(),
                                                astro_entity_portal_link=src_portal_link,
                                                source_ra=source_ra,
                                                source_dec=source_dec,
                                                object_type=object_type,
                                                object_ids=object_ids,
                                                sentry_dsn=sentry_dsn,
                                                update_astro_entity=update_astro_entity,
                                                astro_entity_id=source_entity_id)
        if output_content_post is not None:
            # extract the id of the observation
            astrophysical_entity_drupal_id = output_content_post['nid'][0]['value']
            logger.info(f"Astrophysical entity with id {astrophysical_entity_drupal_id} has been successfully posted")
        else:
            output_content_post = {}
            logger.info(f"no astrophysical entity has been created or updated")

    return output_content_post


def get_observations_for_time_range(product_gallery_url, gallery_jwt_token, t1=None, t2=None, sentry_dsn=None):
    # if None the localtime is assigned
    observations = []
    headers = get_drupal_request_headers(gallery_jwt_token)
    # format the time fields, drupal does not provide (yet) the option to filter by date using also the time,
    # so the dates, properly formatted in ISO8601, without the time will be used
    # and no timezone correction is applied
    # however, drupal provides timezone settings and those are reflected in the dispatcher settings
    t1_parsed = parser.parse(t1)
    t1_formatted = t1_parsed.strftime('%Y-%m-%d')

    t2_parsed = parser.parse(t2)
    t2_formatted = t2_parsed.strftime('%Y-%m-%d')

    log_res = execute_drupal_request(f"{product_gallery_url}/observations/range_t1_t2/{t1_formatted}/{t2_formatted}/",
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)
    output_get = analyze_drupal_output(log_res, operation_performed="getting the observation range")
    if isinstance(output_get, list):
        observations = output_get

    return observations


def post_astro_entity(product_gallery_url, gallery_jwt_token, astro_entity_name,
                      astro_entity_portal_link=None,
                      source_ra=None,
                      source_dec=None,
                      object_type=None,
                      object_ids=None,
                      sentry_dsn=None,
                      astro_entity_id=None,
                      update_astro_entity=False):
    # post new observation with or without a specific time range
    body_gallery_astro_entity_node = copy.deepcopy(body_article_product_gallery.body_node)
    astro_entity_name = astro_entity_name.strip()
    # set the type of content to post
    body_gallery_astro_entity_node["_links"]["type"]["href"] = os.path.join(product_gallery_url,
                                                                            body_gallery_astro_entity_node["_links"]["type"]["href"],
                                                                            'astro_entity')
    # TODO perhaps a bit of duplication here?
    body_gallery_astro_entity_node["title"]["value"] = astro_entity_name
    body_gallery_astro_entity_node["field_source_name"] = [{
        "value": astro_entity_name
    }]
    if astro_entity_portal_link is not None:
        body_gallery_astro_entity_node["field_link"] = [{
            "value": astro_entity_portal_link
        }]
    if object_ids is not None:
        body_gallery_astro_entity_node["field_alternative_names_long_str"] = [{
            "value": ','.join(object_ids)
        }]

    if source_ra is not None:
        body_gallery_astro_entity_node["field_source_ra"] = [{
            "value": source_ra
        }]

    if source_dec is not None:
        body_gallery_astro_entity_node["field_source_dec"] = [{
            "value": source_dec
        }]

    if object_type is not None:
        body_gallery_astro_entity_node["field_object_type"] = [{
            "value": object_type
        }]

    headers = get_drupal_request_headers(gallery_jwt_token)

    if update_astro_entity:
        log_res = execute_drupal_request(os.path.join(product_gallery_url, 'node', astro_entity_id),
                                         method='patch',
                                         data=json.dumps(body_gallery_astro_entity_node),
                                         headers=headers,
                                         sentry_dsn=sentry_dsn)
    else:
        log_res = execute_drupal_request(f"{product_gallery_url}/node",
                                         method='post',
                                         data=json.dumps(body_gallery_astro_entity_node),
                                         headers=headers,
                                         sentry_dsn=sentry_dsn)

    output_post = analyze_drupal_output(log_res, operation_performed="posting a new astrophysical entity")

    return output_post


def build_gallery_observation_node(product_gallery_url,
                                   title,
                                   t1=None, t2=None,
                                   revnum_1=None, revnum_2=None, span_rev=None,
                                   obsids=None,
                                   observation_attachment_file_fid_list=None,
                                   ):
    body_gallery_observation_node = copy.deepcopy(body_article_product_gallery.body_node)
    body_gallery_observation_node["title"]["value"] = title
    # set the type of content to post
    body_gallery_observation_node["_links"]["type"]["href"] = os.path.join(product_gallery_url,
                                                                           body_gallery_observation_node["_links"][
                                                                               "type"][
                                                                               "href"], 'observation')
    if obsids is not None:
        obsids_list = obsids.split(",")
        body_gallery_observation_node['field_obsid'] = []
        for obsid in obsids_list:
            body_gallery_observation_node['field_obsid'].append({
                "value": obsid
            })
    # setting attachments file fid(s) (yaml) if available
    if observation_attachment_file_fid_list is not None:
        for fid in observation_attachment_file_fid_list:
            if 'field_attachments' not in body_gallery_observation_node:
                body_gallery_observation_node['field_attachments'] = []
            body_gallery_observation_node['field_attachments'].append({
                "target_id": int(fid)
            })
    if t1 is not None and t2 is not None:
        body_gallery_observation_node["field_timerange"] = [{
            "value": t1,
            "end_value": t2
        }]

    if revnum_1 is not None:
        body_gallery_observation_node["field_rev1"] = [{
            "value": revnum_1
        }]
    if revnum_2 is not None:
        body_gallery_observation_node["field_rev2"] = [{
            "value": revnum_2
        }]
    if span_rev is not None:
        body_gallery_observation_node["field_span_rev"] = [{
            "value": span_rev
        }]

    return body_gallery_observation_node


def format_time(time_str, tz_to_apply):
    # format the time fields, from the format request
    t_parsed = parser.parse(time_str, ignoretz=True)
    t_formatted = t_parsed.astimezone(tz_to_apply).strftime('%Y-%m-%dT%H:%M:%S%z')

    return t_formatted


def post_observation(product_gallery_url, gallery_jwt_token, converttime_revnum_service_url,
                     title=None,
                     t1=None, t2=None, timezone=None,
                     revnum_1=None, revnum_2=None,
                     obsids=None,
                     observation_attachment_file_fid_list=None,
                     sentry_dsn=None,
                     update_observation=False,
                     create_new=False):
    log_res = output_post = None
    t1_formatted = t2_formatted = t1_revnum_1 = t2_revnum_2 = formatted_title = span_rev = None

    tz_to_apply = tz.gettz(timezone)

    if t1 is not None and t2 is not None:
        # format the time fields, from the format request
        t1_formatted = format_time(t1, tz_to_apply)
        t2_formatted = format_time(t2, tz_to_apply)

        t1_revnum_1 = get_revnum(service_url=converttime_revnum_service_url, time_to_convert=t1_formatted)
        if t1_revnum_1 is not None and 'revnum' in t1_revnum_1:
            t1_revnum_1 = t1_revnum_1['revnum']
        else:
            logger.warning(f'error while retrieving the revolution number from corresponding to the time {t1}')
        t2_revnum_2 = get_revnum(service_url=converttime_revnum_service_url, time_to_convert=t2_formatted)
        if t2_revnum_2 is not None and 'revnum' in t2_revnum_2:
            t2_revnum_2 = t2_revnum_2['revnum']
        else:
            logger.warning(f'error while retrieving the revolution number from corresponding to the time {t2}')

        if t1_revnum_1 is not None and t2_revnum_2 is not None:
            span_rev = t2_revnum_2 - t1_revnum_1

        formatted_title = "_".join(["observation", t1_formatted, t2_formatted])
    else:
        # assign a randomly generate id in case to time range is provided
        formatted_title = "_".join(["observation", str(uuid.uuid4())])

    if title is not None:
        formatted_title=title

    # TODO posting of an observation via the revnum not implemented yet
    # if revnum_1 is not None:
    #     t1_revnum_1 = revnum_1
    # if revnum_2 is not None:
    #     t2_revnum_2 = revnum_2

    body_gallery_observation_node = build_gallery_observation_node(product_gallery_url,
                                                                   title=formatted_title,
                                                                   t1=t1_formatted, t2=t2_formatted,
                                                                   revnum_1=t1_revnum_1, revnum_2=t2_revnum_2,
                                                                   span_rev=span_rev,
                                                                   obsids=obsids,
                                                                   observation_attachment_file_fid_list=observation_attachment_file_fid_list)

    headers = get_drupal_request_headers(gallery_jwt_token)

    if update_observation is False:
        log_res = execute_drupal_request(f"{product_gallery_url}/node",
                                         method='post',
                                         data=json.dumps(body_gallery_observation_node),
                                         headers=headers,
                                         sentry_dsn=sentry_dsn)
    else:
        observation_drupal_id, observation_information_message, output_observation_post = \
            get_observation_drupal_id(product_gallery_url, gallery_jwt_token, converttime_revnum_service_url,
                                      observation_title=title, sentry_dsn=sentry_dsn)
        if observation_drupal_id is not None:
            log_res = execute_drupal_request(os.path.join(product_gallery_url, 'node', observation_drupal_id),
                                             method='patch',
                                             data=json.dumps(body_gallery_observation_node),
                                             headers=headers,
                                             sentry_dsn=sentry_dsn)
        elif observation_drupal_id is None and create_new is True:
            log_res = execute_drupal_request(f"{product_gallery_url}/node",
                                             method='post',
                                             data=json.dumps(body_gallery_observation_node),
                                             headers=headers,
                                             sentry_dsn=sentry_dsn)
    if log_res is not None:
        output_post = analyze_drupal_output(log_res, operation_performed="posting a new observation")

    return output_post


# TODO to further optimize in two separate calls
def get_instrument_product_type_id(product_gallery_url, gallery_jwt_token, product_type=None, instrument=None, sentry_dsn=None) \
        -> Dict:
    output_dict = {}

    headers = get_drupal_request_headers(gallery_jwt_token)
    if product_type is not None or instrument is not None:
        # TODO improve this REST endpoint on drupal to accept multiple input terms, and give one result per input
        # get all the taxonomy terms
        log_res = execute_drupal_request(f"{product_gallery_url}/taxonomy/term_name/all?_format=hal_json",
                                         headers=headers,
                                         sentry_dsn=sentry_dsn)
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


def get_all_revolutions(product_gallery_url, gallery_jwt_token, sentry_dsn=None) -> Optional[list]:
    entities = []
    headers = get_drupal_request_headers(gallery_jwt_token)
    log_res = execute_drupal_request(f"{product_gallery_url}/get_revs",
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)
    output_get = analyze_drupal_output(log_res, operation_performed="retrieving all the revolutions from the product gallery")
    if isinstance(output_get, list):
        entities = list(obj['title'] for obj in output_get)

    return entities


def get_data_product_list_by_source_name(product_gallery_url, gallery_jwt_token, src_name=None, sentry_dsn=None) -> Optional[list]:
    product_list = []
    if src_name is None:
        return  product_list
    headers = get_drupal_request_headers(gallery_jwt_token)

    source_entity_list = get_source_astrophysical_entity_info_by_source_and_alternative_name(product_gallery_url,
                                                                                         gallery_jwt_token,
                                                                                         source_name=src_name,
                                                                                         sentry_dsn=sentry_dsn)

    source_entity_id = None
    if len(source_entity_list) >= 1:
        source_entity_id = source_entity_list[0]['nid']

    if source_entity_id is not None:
        log_res = execute_drupal_request(f"{product_gallery_url}/data_products/source_products/{source_entity_id}",
                                         headers=headers,
                                         sentry_dsn=sentry_dsn)
        output_get = analyze_drupal_output(log_res, operation_performed="retrieving the astrophysical entity information")
        if isinstance(output_get, list):
            for obj in output_get:
                refactored_obj = {}
                for k, v in obj.items():
                    refactored_key = k
                    if k.startswith('field_'):
                        refactored_key = k.replace('field_', '')
                    refactored_obj[refactored_key] = v
                product_list.append(refactored_obj)

    return product_list


def get_all_source_astrophysical_entities(product_gallery_url, gallery_jwt_token, sentry_dsn=None) -> Optional[list]:
    entities = []
    headers = get_drupal_request_headers(gallery_jwt_token)
    log_res = execute_drupal_request(f"{product_gallery_url}/astro_entities/source/all",
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)
    output_get = analyze_drupal_output(log_res, operation_performed="retrieving the astrophysical entity information")
    if isinstance(output_get, list):
        entities = list({'title': obj['title'], 'ra': obj['field_source_ra'], 'dec': obj['field_source_dec']} for obj in output_get)

    return entities


def get_source_astrophysical_entity_id_by_source_name(product_gallery_url, gallery_jwt_token, source_name=None, sentry_dsn=None) \
        -> Optional[str]:
    entities_id = None
    headers = get_drupal_request_headers(gallery_jwt_token)

    # the URL-reserved characters should be quoted eg GX 1+4 -> GX%201%2B4
    source_name = urllib.parse.quote(source_name.strip())
    log_res = execute_drupal_request(f"{product_gallery_url}/astro_entities/source/{source_name}",
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)
    output_get = analyze_drupal_output(log_res, operation_performed="retrieving the astrophysical entity information")
    if isinstance(output_get, list) and len(output_get) == 1:
        entities_id = output_get[0]['nid']

    return entities_id


# TODO to verify if it will always return one single object for a single source name (or alternative name)
def get_source_astrophysical_entity_info_by_source_and_alternative_name(product_gallery_url, gallery_jwt_token, source_name=None, sentry_dsn=None) \
        -> Optional[list]:
    # get from the drupal the relative id
    headers = get_drupal_request_headers(gallery_jwt_token)
    output_get = {}

    if source_name is not None:
        # TODO to verify if quoting is needed eg GX 1+4 -> GX%201%2B4
        params = {"src_name": source_name.strip(),
                  "_format": "hal_json"}

        log_res = execute_drupal_request(f"{product_gallery_url}/astro_entities/all_sources",
                                         headers=headers,
                                         params=params,
                                         sentry_dsn=sentry_dsn)
        output_get = analyze_drupal_output(log_res, operation_performed="retrieving the astrophysical entity information")

    return output_get


def get_data_product_list_by_job_id(product_gallery_url, gallery_jwt_token, job_id=None, sentry_dsn=None) -> list:
    data_product_list = []
    # get from the drupal the relative id
    headers = get_drupal_request_headers(gallery_jwt_token)

    log_res = execute_drupal_request(f"{product_gallery_url}/data_products/job_id/{job_id}",
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)
    output_get = analyze_drupal_output(log_res, operation_performed="retrieving the list of data product for a given job_id")
    if isinstance(output_get, list):
        data_product_list = output_get

    return data_product_list


def get_data_product_list_by_product_id(product_gallery_url, gallery_jwt_token, product_id=None, sentry_dsn=None) -> list:
    data_product_list = []
    # get from the drupal the relative id
    headers = get_drupal_request_headers(gallery_jwt_token)

    log_res = execute_drupal_request(f"{product_gallery_url}/data_products/product_id/{product_id}",
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)
    output_get = analyze_drupal_output(log_res, operation_performed="retrieving the list of data product for a given product_id")
    if isinstance(output_get, list):
        data_product_list = output_get

    return data_product_list


def get_observation_yaml_attachments_by_observation_title(product_gallery_url, gallery_jwt_token, observation_title,
                                                          sentry_dsn=None):
    # get from the drupal the relative id
    headers = get_drupal_request_headers(gallery_jwt_token)

    log_res = execute_drupal_request(f"{product_gallery_url}/get_observation_attachments/{observation_title}",
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)
    output_get = analyze_drupal_output(log_res, operation_performed="retrieving the observation attachments")

    if output_get is not None and isinstance(output_get, list):
        # TODO might be needed if a better formatting of the output is needed
        # if 'file_content' in output_get[0]:
        #     splitted_content_list = output_get[0]['file_content'].split('{"single_file_content": "')
        #     for splitted_content in splitted_content_list:
        #         if splitted_content != '':
        #             if splitted_content.strip()
        #             yaml.parse(splitted_content)
        return output_get[0]

    return output_get


def get_observation_drupal_id(product_gallery_url, gallery_jwt_token, converttime_revnum_service_url,
                              t1=None, t2=None, timezone=None,
                              revnum_1=None, revnum_2=None,
                              obsids=None,
                              observation_attachment_file_fid_list=None,
                              observation_title=None,
                              sentry_dsn=None) \
        -> Tuple[Optional[str], Optional[str], Optional[object]]:
    observation_drupal_id = None
    observation_information_message = None
    observation_information_message_timezone_warning = ""
    output_post = None
    if observation_title is not None:
        # get from the drupal the relative id
        headers = get_drupal_request_headers(gallery_jwt_token)

        log_res = execute_drupal_request(f"{product_gallery_url}/observations/{observation_title}",
                                         headers=headers,
                                         sentry_dsn=sentry_dsn)
        output_get = analyze_drupal_output(log_res, operation_performed="retrieving the observation information")

        if isinstance(output_get, list) and len(output_get) >= 1:
            observation_drupal_id = output_get[0]['nid']
            observation_information_message = 'observation assigned by the user'

    else:

        if t1 is not None and t2 is not None:
            parsed_t1 = parser.parse(t1)
            parsed_t2 = parser.parse(t2)
            parsed_t1_no_timezone = parsed_t1.strftime('%Y-%m-%dT%H:%M:%S')
            parsed_t2_no_timezone = parsed_t2.strftime('%Y-%m-%dT%H:%M:%S')
            if parsed_t1.tzinfo is not None or parsed_t2.tzinfo is not None:
                observation_information_message_timezone_warning = ', it has been noticed that the time provided contains timezone information, ' \
                                                   'we will ignore those when processing those data'
            logger.info(f"searching over the gallery for a period of observation with the following time range: "
                        f"{t1} - {t2}")
            observations_range = get_observations_for_time_range(product_gallery_url, gallery_jwt_token, t1=t1, t2=t2, sentry_dsn=sentry_dsn)
            for observation in observations_range:
                # parse times returned from drupal
                times = observation['field_timerange'].split('--')
                if len(times) > 0:
                    t_start_no_timezone = parser.parse(times[0]).strftime('%Y-%m-%dT%H:%M:%S')
                    if len(times) == 1:
                        t_end_no_timezone = t_start_no_timezone
                    else:
                        t_end_no_timezone = parser.parse(times[1]).strftime('%Y-%m-%dT%H:%M:%S')
                else:
                    observation_information_message = 'no valid observation has been found'
                    break
                logger.info(f"comparing time range extracted from Drupal: {t_start_no_timezone} - {t_end_no_timezone}")
                if t_start_no_timezone == parsed_t1_no_timezone and t_end_no_timezone == parsed_t2_no_timezone:
                    observation_drupal_id = observation['nid']
                    observation_information_message = 'observation assigned from the provided time range' + \
                                                      observation_information_message_timezone_warning
                    break

    if observation_drupal_id is None and (t1 is not None and t2 is not None):
        output_post = post_observation(product_gallery_url, gallery_jwt_token, converttime_revnum_service_url,
                                       title=observation_title,
                                       t1=t1, t2=t2, timezone=timezone,
                                       obsids=obsids,
                                       observation_attachment_file_fid_list=observation_attachment_file_fid_list,
                                       sentry_dsn=sentry_dsn)
        # extract the id of the observation
        if output_post is not None:
            observation_drupal_id = output_post['nid'][0]['value']
            observation_information_message = 'a new observation has been posted' + \
                                                  observation_information_message_timezone_warning

    return observation_drupal_id, observation_information_message, output_post


def post_revolution_processing_log_to_gallery(product_gallery_url, gallery_jwt_token,
                                              user_id_product_creator=None,
                                              sentry_dsn=None,
                                              **kwargs):
    body_gallery_article_node = copy.deepcopy(body_article_product_gallery.body_node)

    # set the type of content to post
    body_gallery_article_node["_links"]["type"]["href"] = os.path.join(product_gallery_url,
                                                                       body_gallery_article_node["_links"]["type"][
                                                                           "href"], 'data_product')

    # set the initial body content
    body_gallery_article_node["body"][0]["value"] = ''

    # set the user id of the author of the data product
    if user_id_product_creator is not None:
        body_gallery_article_node["uid"] = [{
            "target_id": user_id_product_creator
        }]

    # body_gallery_article_node["title"]["value"] = "revnumber_status_log"

    # let's go through the kwargs and if any overwrite some values for the product to post
    for k, v in kwargs.items():
        # assuming the name of the field in drupal starts always with field_
        field_name = str.lower('field_' + k)
        body_gallery_article_node[field_name] = [{
            "value": v
        }]

        # finally, post the data product to the gallery
    headers = get_drupal_request_headers(gallery_jwt_token)

    logger.info("posting a new revolution-processing-log")
    log_res = execute_drupal_request(os.path.join(product_gallery_url, 'node', ),
                                     method='post',
                                     data=json.dumps(body_gallery_article_node),
                                     headers=headers,
                                     sentry_dsn=sentry_dsn)
    output_post = analyze_drupal_output(log_res, operation_performed="posting a new revolution processing log to the gallery")

    return output_post


def post_data_product_to_gallery(product_gallery_url, gallery_jwt_token, converttime_revnum_service_url,
                                 data_product_id=None,
                                 product_title=None,
                                 img_fid=None,
                                 fits_file_fid_list=None,
                                 html_file_fid_list=None,
                                 observation_attachment_file_fid_list=None,
                                 observation_id=None,
                                 user_id_product_creator=None,
                                 insert_new_source=False,
                                 sentry_dsn=None,
                                 timezone=None,
                                 **kwargs):
    body_gallery_article_node = copy.deepcopy(body_article_product_gallery.body_node)

    # set the type of content to post
    body_gallery_article_node["_links"]["type"]["href"] = os.path.join(product_gallery_url, body_gallery_article_node["_links"]["type"][
                                                                  "href"], 'data_product')

    # set the initial body content
    body_value = ''
    t1 = t2 = instrument = product_type = obsid = None

    job_id = kwargs.get('job_id', None)

    if job_id is not None:
        # in case job_id is passed then it automatically extracts time, instrument and product_type information
        # related to the specific job, and uses them unless provided by the user

        job_id_scratch_dir_list = glob.glob(f'scratch_sid_*_jid_{job_id}*')
        analysis_parameters_json_content_original = None

        if len(job_id_scratch_dir_list) >= 1:
            analysis_parameters_json_content_original = json.load(open(os.path.join(job_id_scratch_dir_list[0], 'analysis_parameters.json')))
        else:
            logger.warning(f'no results folder could be found with the provided job_id ({job_id}),'
                           f' perhaps wrong job_id was passed?')
            raise RequestNotUnderstood(message="Request data not found",
                                       payload={'drupal_helper_error_message': 'error while posting data product: '
                                                                               'no results folder could be found with the provided job_id, '
                                                                               'perhaps wrong job_id was passed?'})

        if analysis_parameters_json_content_original is not None:
            instrument = analysis_parameters_json_content_original.pop('instrument')
            product_type = analysis_parameters_json_content_original.pop('product_type')
            # time data for the observation
            t1 = analysis_parameters_json_content_original.pop('T1')
            t2 = analysis_parameters_json_content_original.pop('T2')
        else:
            logger.warning(f'no analysis_parameters.json file was found inside the scratch folder {job_id_scratch_dir_list[0]},'
                           f' this can be related to an internal error')
            raise RequestNotUnderstood(message="Request data not found",
                                       payload={'drupal_helper_error_message': 'error while posting data product: '
                                                                               'request deta for the provided job_id could not be found, '
                                                                               'this can be related to an internal error'})

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
    if 'obsid' in kwargs:
        obsid = kwargs.pop('obsid')

    observation_drupal_id, observation_information_message, output_observation_post = get_observation_drupal_id(product_gallery_url, gallery_jwt_token,
                                                                                       converttime_revnum_service_url,
                                                                                       t1=t1, t2=t2, timezone=timezone,
                                                                                       obsids=obsid,
                                                                                       observation_attachment_file_fid_list=observation_attachment_file_fid_list,
                                                                                       observation_title=observation_id,
                                                                                       sentry_dsn=sentry_dsn)
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

    # set the source astrophysical entity if available
    src_name_concat = None
    src_name_arg = kwargs.pop('src_name', None)
    src_portal_link_arg = kwargs.pop('entity_portal_link_list', None)
    object_ids_arg = kwargs.pop('object_ids_list', None)
    source_coord_arg = kwargs.pop('source_coord_list', None)
    object_type_arg = kwargs.pop('object_type_list', None)
    if src_name_arg is not None:
        src_name_list = src_name_arg.split(',')
        src_name_concat = "_".join(src_name_list)

        src_portal_link_list = None
        if src_portal_link_arg is not None:
            # TODO consider using json.loads
            src_portal_link_list = src_portal_link_arg.split(',')

        object_ids_lists = None
        if object_ids_arg is not None:
            object_ids_lists = json.loads(object_ids_arg)

        source_coord_obj_list = None
        if source_coord_arg is not None:
            source_coord_obj_list = json.loads(source_coord_arg)

        object_type_list = None
        if object_type_arg is not None:
            object_type_list = json.loads(object_type_arg)

        for src_name in src_name_list:
            src_name_idx = src_name_list.index(src_name)
            arg_source_coord = {}
            if source_coord_obj_list is not None and source_coord_obj_list[src_name_idx] != {}:
                arg_source_coord = source_coord_obj_list[src_name_idx]
            source_entity_list = get_source_astrophysical_entity_info_by_source_and_alternative_name(product_gallery_url, gallery_jwt_token,
                                                                                   source_name=src_name,
                                                                                   sentry_dsn=sentry_dsn)
            source_entity_id = None
            if len(source_entity_list) == 1:
                source_entity_id = source_entity_list[0]['nid']
            elif len(source_entity_list) > 1:
                for source_entity in source_entity_list:
                    source_entity_title = source_entity['title']
                    if source_entity_title.strip() == src_name.strip():
                        source_entity_id = source_entity['nid']
                        break
                    else:
                        source_entity_coord_ra = None
                        source_entity_coord_dec = None
                        if source_entity['field_source_ra'] != "":
                            source_entity_coord_ra = float(source_entity['field_source_ra'])
                        if source_entity['field_source_dec'] != "":
                            source_entity_coord_dec = float(source_entity['field_source_dec'])

                        arg_source_coord_ra = arg_source_coord.get('source_ra', None)
                        arg_source_coord_dec = arg_source_coord.get('source_dec', None)
                        if source_entity_coord_ra is not None and source_entity_coord_dec is not None and \
                                arg_source_coord_ra is not None and arg_source_coord_dec is not None:
                            matching_coords = check_matching_coords(source_entity_title, source_entity_coord_ra, source_entity_coord_dec,
                                                                    src_name, arg_source_coord_ra, arg_source_coord_dec)
                            if matching_coords:
                                source_entity_id = source_entity['nid']
                                break

            # create a new source ? yes if the user wants it
            if source_entity_id is None and insert_new_source:
                src_portal_link = None
                if src_portal_link_list is not None and src_portal_link_list[src_name_idx] != '':
                    src_portal_link = src_portal_link_list[src_name_idx].strip()
                object_ids = None
                if object_ids_lists is not None and object_ids_lists[src_name_idx] != []:
                    object_ids = object_ids_lists[src_name_idx]
                object_type = None
                if object_type_list is not None and object_type_list[src_name_idx] != '':
                    object_type = object_type_list[src_name_idx]
                output_post = post_astro_entity(product_gallery_url, gallery_jwt_token,
                                                astro_entity_name=src_name.strip(),
                                                astro_entity_portal_link=src_portal_link,
                                                source_ra=arg_source_coord.get('source_ra', None),
                                                source_dec=arg_source_coord.get('source_dec', None),
                                                object_type=object_type,
                                                object_ids=object_ids,
                                                sentry_dsn=sentry_dsn)

                # extract the id of the observation
                source_entity_id = output_post['nid'][0]['value']

            if source_entity_id is not None:
                if 'field_describes_astro_entity' not in body_gallery_article_node:
                    body_gallery_article_node['field_describes_astro_entity'] = []
                body_gallery_article_node['field_describes_astro_entity'].append({
                    "target_id": int(source_entity_id)
                })

    # set the product title
    # TODO agree on a better logic to assign the product title, have it mandatory?
    if product_title is None:
        if product_type is None and src_name_concat is None:
            product_title = "_".join(["data_product", str(uuid.uuid4())])
        elif product_type is None and src_name_concat is not None:
            product_title = src_name_concat
        elif product_type is not None and src_name_concat is None:
            product_title = product_type
        else:
            product_title = "_".join([src_name_concat, product_type])

    body_gallery_article_node["title"]["value"] = product_title

    ids_obj = get_instrument_product_type_id(product_gallery_url=product_gallery_url,
                                             gallery_jwt_token=gallery_jwt_token,
                                             product_type=product_type,
                                             instrument=instrument,
                                             sentry_dsn=sentry_dsn)
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
    # setting html file fid if available
    if html_file_fid_list is not None:
        for fid in html_file_fid_list:
            if 'field_html_file' not in body_gallery_article_node:
                body_gallery_article_node['field_html_file'] = []
            body_gallery_article_node['field_html_file'].append({
                "target_id": int(fid)
            })

    # finally, post the data product to the gallery
    headers = get_drupal_request_headers(gallery_jwt_token)

    if data_product_id is not None:
        logger.info(f"updating the data-product with id {data_product_id}")
        log_res = execute_drupal_request(os.path.join(product_gallery_url, 'node', data_product_id),
                                         method='patch',
                                         data=json.dumps(body_gallery_article_node),
                                         headers=headers,
                                         sentry_dsn=sentry_dsn)
        output_post = analyze_drupal_output(log_res, operation_performed=f"updating the data-product with id {data_product_id}")
    else:
        logger.info("posting a new data-product")
        log_res = execute_drupal_request(os.path.join(product_gallery_url, 'node', ),
                                         method='post',
                                         data=json.dumps(body_gallery_article_node),
                                         headers=headers,
                                         sentry_dsn=sentry_dsn)
        output_post = analyze_drupal_output(log_res, operation_performed="posting a new data product to the gallery")

    return output_post


def check_matching_coords(source_1_name, source_1_coord_ra, source_1_coord_dec,
                          source_2_name, source_2_coord_ra, source_2_coord_dec,
                          tolerance=1. / 60):
    drupal_source_sky_coord = SkyCoord(source_1_coord_ra, source_1_coord_dec, unit=(u.hourangle, u.deg))
    arg_source_sky_coord = SkyCoord(source_2_coord_ra, source_2_coord_dec, unit=(u.hourangle, u.deg), frame="fk5")
    separation = drupal_source_sky_coord.separation(arg_source_sky_coord).deg
    ind = np.logical_or(source_1_name == source_2_name, separation <= tolerance)
    if np.count_nonzero(ind) > 0:
        return True

    return False


def resolve_name(name_resolver_url: str, entities_portal_url: str = None, name: str = None):
    resolved_obj = {}
    if name is not None:
        quoted_name = urllib.parse.quote(name.strip())
        res = requests.get(name_resolver_url.format(quoted_name))
        if res.status_code == 200:
            returned_resolved_obj = res.json()
            if 'success' in returned_resolved_obj:
                resolved_obj['name'] = name.replace('_', ' ')
                if returned_resolved_obj['success']:
                    logger.info(f"object {name} successfully resolved")
                    if 'ra' in returned_resolved_obj:
                        resolved_obj['RA'] = float(returned_resolved_obj['ra'])
                    if 'dec' in returned_resolved_obj:
                        resolved_obj['DEC'] = float(returned_resolved_obj['dec'])
                    if 'object_ids' in returned_resolved_obj:
                        resolved_obj['object_ids'] = returned_resolved_obj['object_ids']
                    if 'object_type' in returned_resolved_obj:
                        resolved_obj['object_type'] = returned_resolved_obj['object_type']
                    resolved_obj['entity_portal_link'] = entities_portal_url.format(quoted_name)
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
                                payload={'drupal_helper_error_message': res.text})
    return resolved_obj


def get_revnum(service_url: str, time_to_convert: str = None):
    resolved_obj = {}
    try:
        if time_to_convert is None or time_to_convert == '':
            time_to_convert = datetime.now()
        else:
            time_to_convert = parser.parse(time_to_convert)
        time_to_convert = time_to_convert.strftime('%Y-%m-%dT%H:%M:%S')
    except parser.ParserError as e:
        logger.warning(
            f"error while parsing the time {time_to_convert}, "
            f"please check your request and try to issue it again")
        return resolved_obj
    res = requests.get(service_url.format(time_to_convert))

    if res.status_code == 200:
        resolved_obj['revnum'] = int(res.content)
    else:
        logger.warning(f"there seems to be some problem in completing the request for the conversion of the time: {time_to_convert}\n"
                       f"the request lead to the error {res.text}, "
                       "this might be due to an error in the url or the service "
                       "requested is currently not available, "
                       "please check your request and try to issue it again")
        raise InternalError('issue when performing a request to the timesystem service',
                            status_code=500,
                            payload={'drupal_helper_error_message': res.text})
    return resolved_obj
