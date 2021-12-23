import os
import json
import requests
import base64
import copy
import uuid
import time as _time

from datetime import datetime
from dateutil import parser
from enum import Enum, auto

from cdci_data_analysis.analysis import email_helper
from ..analysis.exceptions import RequestNotUnderstood
from ..flask_app.templates import body_article_product_gallery


class ContentType(Enum):
    ARTICLE = auto()
    DATA_PRODUCT = auto()
    OBSERVATION = auto()
    ASTROPHYSICAL_ENTITY = auto()


def get_mmoda_pg_token(jwt_token__file_path):
    if os.path.exists(os.path.join(os.getcwd(), jwt_token__file_path)):
        return open(os.path.join(os.getcwd(), jwt_token__file_path)).read().strip()
    return ''


def get_user_id(product_gallery_url, user_email, jwt_token) -> str:
    user_id = None
    headers = {
        'Content-type': 'application/hal+json',
        'Authorization': 'Bearer ' + jwt_token
    }

    # get the user id
    log_res = requests.get(f"{product_gallery_url}/users/{user_email}?_format=hal_json",
                           headers=headers
                           )
    output_get = log_res.json()
    if log_res.status_code < 200 or log_res.status_code >= 300:
        raise RequestNotUnderstood(output_get['message'],
                                   status_code=log_res.status_code,
                                   payload={'error_message': 'error while retrieving the user id'})
    if isinstance(output_get, list) and len(output_get) == 1:
        user_id = output_get[0]['uid']

    return user_id


def post_picture_to_gallery(product_gallery_url, img, jwt_token):
    # body_post_img = body_article_product_gallery.body_img.copy()
    body_post_img = copy.deepcopy(body_article_product_gallery.body_img)

    bytes_img = img.read()
    b_64_img = base64.b64encode(bytes_img).decode("utf8")
    img_name = img.filename
    img_extension = os.path.splitext(img_name)[1][1:]

    body_post_img["data"][0]["value"] = b_64_img
    body_post_img["uri"][0]["value"] = "public://" + img_name
    body_post_img["filename"][0]["value"] = img_name
    body_post_img["filemime"]["value"] = "image/" + img_extension
    body_post_img["_links"]["type"]["href"] = f"{product_gallery_url}/rest/type/file/image"

    headers = {
        'Content-type': 'application/hal+json',
        'Authorization': 'Bearer ' + jwt_token
    }

    # post the image
    log_res = requests.post(f"{product_gallery_url}/entity/file?_format=hal_json",
                            data=json.dumps(body_post_img),
                            headers=headers
                            )
    output_post = log_res.json()
    if log_res.status_code < 200 or log_res.status_code >= 300:
        raise RequestNotUnderstood(output_post['message'],
                                   status_code=log_res.status_code,
                                   payload={'error_message': 'error while posting article'})
    return output_post


def post_content_to_gallery(product_gallery_url,
                            jwt_token,
                            **kwargs):
    # extract type of content to post
    content_type = ContentType[str.upper(kwargs.pop('content_type', 'article'))]
    if content_type == content_type.DATA_PRODUCT:
        session_id = kwargs.pop('session_id')
        job_id = kwargs.pop('job_id')
        product_title = kwargs.pop('product_title', None)
        img_fid = kwargs.pop('img_fid', None)
        observation_id = kwargs.pop('observation_id', None)
        user_id_product_creator = kwargs.pop('user_id_product_creator')
        return post_data_product_to_gallery(product_gallery_url=product_gallery_url,
                                            session_id=session_id,
                                            job_id=job_id,
                                            jwt_token=jwt_token,
                                            product_title=product_title,
                                            img_fid=img_fid,
                                            observation_id=observation_id,
                                            user_id_product_creator=user_id_product_creator,
                                            **kwargs)


def get_observation_drupal_id(product_gallery_url, jwt_token, t1=None, t2=None, observation_id=None):
    observation_drupal_id = None
    if observation_id is not None:
        # get from the drupal the relative id
        headers = {
            'Content-type': 'application/hal+json',
            'Authorization': 'Bearer ' + jwt_token
        }
        # post the article
        log_res = requests.get(f"{product_gallery_url}/observations/{observation_id}?_format=hal_json",
                               headers=headers
                               )
        output_get = log_res.json()
        if log_res.status_code < 200 or log_res.status_code >= 300:
            raise RequestNotUnderstood(output_get['message'],
                                       status_code=log_res.status_code,
                                       payload={'error_message': 'error while retrieving the user id'})
        if isinstance(output_get, list) and len(output_get) == 1:
            observation_drupal_id = output_get[0]['nid']
    else:
        # post new observation with or without a specific time range
        # body_gallery_observation_node = body_article_product_gallery.body_article.copy()
        body_gallery_observation_node = copy.deepcopy(body_article_product_gallery.body_article)
        # set the type of content to post
        body_gallery_observation_node["_links"]["type"]["href"] = body_gallery_observation_node["_links"]["type"]["href"] + 'observation'

        if t1 is not None and t2 is not None:
            # check if noe observation with that daterange already exists

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

        headers = {
            'Content-type': 'application/hal+json',
            'Authorization': 'Bearer ' + jwt_token
        }
        # post the article
        log_res = requests.post(f"{product_gallery_url}/node?_format=hal_json",
                                data=json.dumps(body_gallery_observation_node),
                                headers=headers
                                )
        output_post = log_res.json()
        if log_res.status_code < 200 or log_res.status_code >= 300:
            raise RequestNotUnderstood(output_post['message'],
                                       status_code=log_res.status_code,
                                       payload={'error_message': 'error while posting article'})

        # extract the id of the observation
        observation_drupal_id = output_post['nid'][0]['value']
    return observation_drupal_id


def post_data_product_to_gallery(product_gallery_url, session_id, job_id, jwt_token,
                                 product_title=None,
                                 img_fid=None,
                                 observation_id=None,
                                 user_id_product_creator=None,
                                 **kwargs):
    # body_gallery_article_node = body_article_product_gallery.body_article.copy()
    body_gallery_article_node = copy.deepcopy(body_article_product_gallery.body_article)

    # set the type of content to post
    link_content_type = body_gallery_article_node["_links"]["type"]["href"] + 'data_product'
    body_gallery_article_node["_links"]["type"]["href"] = link_content_type

    # set the product title
    if product_title is None:
        product_title = ''
    # set the initial body content
    body_value = ''
    current_time_formatted = datetime.fromtimestamp(_time.time()).strftime("%Y-%m-%d %H:%M:%S")
    product_title = "_".join([product_title, 'data_product', current_time_formatted])

    body_gallery_article_node["title"]["value"] = product_title

    # get products
    scratch_dir_json_fn = f'scratch_sid_{session_id}_jid_{job_id}'
    # the aliased version might have been created
    scratch_dir_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased'
    analysis_parameters_json_content_original = None
    #
    if os.path.exists(scratch_dir_json_fn):
        analysis_parameters_json_content_original = json.load(open(scratch_dir_json_fn + '/analysis_parameters.json'))
    elif os.path.exists(scratch_dir_json_fn_aliased):
        analysis_parameters_json_content_original = json.load(
            open(scratch_dir_json_fn_aliased + '/analysis_parameters.json'))

    if analysis_parameters_json_content_original is not None:
        analysis_parameters_json_content_original.pop('token', None)
        analysis_parameters_json_content_original_str = email_helper.wrap_python_code(
            json.dumps(analysis_parameters_json_content_original))
        instrument = analysis_parameters_json_content_original['instrument']
        # set the energy range
        e1_kev = analysis_parameters_json_content_original['E1_keV']
        if e1_kev is not None:
            body_gallery_article_node["field_e1_kev"] = [{
                "value": e1_kev
            }]
        e2_kev = analysis_parameters_json_content_original['E2_keV']
        if e2_kev is not None:
            body_gallery_article_node["field_e2_kev"] = [{
                "value": e2_kev
            }]
        product_type = analysis_parameters_json_content_original['product_type']
        # set the observation information by default if not provided by the user

        t1 = analysis_parameters_json_content_original['T1']
        t2 = analysis_parameters_json_content_original['T2']
        observation_drupal_id = get_observation_drupal_id(product_gallery_url, jwt_token,
                                                          t1=t1, t2=t2, observation_id=observation_id)
        body_gallery_article_node["field_derived_from_observation"] = [{
            "target_id": observation_drupal_id
        }]

        body_value = (f'''''')
    else:
        raise RequestNotUnderstood(message="Request data ont found",
                                   payload={'error_message': 'error while posting article'})

    # let's go through the kwargs and if any overwrite some values for the product to post
    for k, v in kwargs.items():
        # assuming the name of the field in drupal starts always with field_
        field_name = 'field_' + k
        body_gallery_article_node[field_name] = [{
            "value": v
        }]

    body_gallery_article_node["body"][0]["value"] = body_value

    # set the user id of the author of the data product
    if user_id_product_creator is not None:
        body_gallery_article_node["uid"] = [{
            "target_id": user_id_product_creator
        }]

    headers = {
        'Content-type': 'application/hal+json',
        'Authorization': 'Bearer ' + jwt_token
    }
    # TODO improve this REST endpoint to accept multiple input terms, and give one result per input
    # get all the taxonomy terms
    log_res = requests.get(f"{product_gallery_url}/taxonomy/term_name/all",
                           headers=headers
                           )
    output_post = log_res.json()
    if type(output_post) == list and len(output_post) > 0:
        for output in output_post:
            if output['vid'] == 'Instruments' and output['name'] == instrument:
                # info for the instrument
                body_gallery_article_node['field_instrumentused'] = [{
                    "target_id": int(output['tid'])
                }]
            if output['vid'] == 'product_type' and output['name'] == product_type:
                # info for the product
                body_gallery_article_node['field_data_product_type'] = [{
                    "target_id": int(output['tid'])
                }]

    # setting img fid if available
    if img_fid is not None:
        body_gallery_article_node['field_image_png'] = [{
            "target_id": int(img_fid)
        }]
    # post the article
    log_res = requests.post(f"{product_gallery_url}/node?_format=hal_json",
                            data=json.dumps(body_gallery_article_node),
                            headers=headers
                            )
    output_post = log_res.json()
    if log_res.status_code < 200 or log_res.status_code >= 300:
        raise RequestNotUnderstood(output_post['message'],
                                   status_code=log_res.status_code,
                                   payload={'error_message': 'error while posting article'})

    return output_post
