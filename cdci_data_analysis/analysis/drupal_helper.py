import os
import json
import requests
import base64
import time as _time

from datetime import datetime
from enum import Enum, auto

from cdci_data_analysis.analysis import email_helper
from ..analysis.exceptions import RequestNotUnderstood
from ..flask_app.templates import body_article_product_gallery


class ContentType(Enum):
    ARTICLE = auto()
    DATA_PRODUCT = auto()
    OBSERVATION = auto()
    ASTROPHYSICAL_ENTITY = auto()


def discover_mmoda_pg_token():
    if os.path.exists(os.path.join(os.getcwd(), ".mmoda-pg-token")):
        return open(os.path.join(os.getcwd(), ".mmoda-pg-token")).read().strip()
    return ''


def post_picture_to_gallery(img, jwt_token):
    body_post_img = body_article_product_gallery.body_img.copy()
    bytes_img = img.read()
    b_64_img = base64.b64encode(bytes_img).decode("utf8")
    img_name = img.filename
    img_extension = os.path.splitext(img_name)[1][1:]

    body_post_img["data"][0]["value"] = b_64_img
    body_post_img["uri"][0]["value"] = "public://" + img_name
    body_post_img["filename"][0]["value"] = img_name
    body_post_img["filemime"]["value"] = "image/" + img_extension
    body_post_img["_links"]["type"]["href"] = "http://cdciweb02.isdc.unige.ch/mmoda-pg/rest/type/file/image"

    headers = {
        'Content-type': 'application/hal+json',
        'Authorization': 'Bearer ' + jwt_token
    }

    # post the image
    log_res = requests.post("http://cdciweb02.isdc.unige.ch/mmoda-pg/entity/file?_format=hal_json",
                            data=json.dumps(body_post_img),
                            headers=headers
                            )
    output_post = log_res.json()
    if log_res.status_code < 200 or log_res.status_code >= 300:
        raise RequestNotUnderstood(output_post['message'],
                                   status_code=log_res.status_code,
                                   payload={'error_message': 'error while posting article'})
    return output_post


def post_to_product_gallery(session_id, job_id, jwt_token, product_title=None, content_type=ContentType.ARTICLE, img_fid=None):
    body_gallery_article_node = body_article_product_gallery.body_article.copy()

    # set the type of content to post
    link_content_type = body_gallery_article_node["_links"]["type"]["href"] + str.lower(content_type.name)
    body_gallery_article_node["_links"]["type"]["href"] = link_content_type

    # set the product title
    if product_title is None:
        product_title = ''

    current_time_formatted = datetime.fromtimestamp(_time.time()).strftime("%Y-%m-%d %H:%M:%S")
    product_title = "_".join([product_title, str.lower(content_type.name), current_time_formatted])

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
        body_value = 'Body with the analysis_parameters.json: <br/><br/>' \
                     '<div style="background-color: lightgray; display: inline-block; padding: 5px;">' + \
                     analysis_parameters_json_content_original_str.replace("\n", "<br>") + '</div>'
        instrument = analysis_parameters_json_content_original['instrument']
        product_type = analysis_parameters_json_content_original['product_type']
    else:
        raise RequestNotUnderstood(message="Request data ont found",
                                   payload={'error_message': 'error while posting article'})

    body_gallery_article_node["body"][0]["value"] = body_value

    headers = {
        'Content-type': 'application/hal+json',
        'Authorization': 'Bearer ' + jwt_token
    }
    # TODO improve this REST endpoint to accept multiple input terms, and give one result per input
    # get all the taxonomy terms
    log_res = requests.get("http://cdciweb02.isdc.unige.ch/mmoda-pg/taxonomy/term_name/all",
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
        img_field_key = 'field_image'
        if content_type == ContentType.DATA_PRODUCT:
            img_field_key = 'field_image_png'

        body_gallery_article_node[img_field_key] = [{
            "target_id": int(img_fid)
        }]
    # post the article
    log_res = requests.post("http://cdciweb02.isdc.unige.ch/mmoda-pg/node?_format=hal_json",
                            data=json.dumps(body_gallery_article_node),
                            headers=headers
                            )
    output_post = log_res.json()
    if log_res.status_code < 200 or log_res.status_code >= 300:
        raise RequestNotUnderstood(output_post['message'],
                                   status_code=log_res.status_code,
                                   payload={'error_message': 'error while posting article'})

    return output_post
