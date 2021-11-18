import os
import json
import requests

from cdci_data_analysis.analysis import email_helper
from ..analysis.exceptions import RequestNotUnderstood
from ..flask_app.templates import body_article_product_gallery


def post_to_product_gallery(session_id, job_id):
    body_gallery_node = body_article_product_gallery.body.copy()
    # get products
    scratch_dir_json_fn = f'scratch_sid_{session_id}_jid_{job_id}'
    # the aliased version might have been created
    scratch_dir_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased'
    analysis_parameters_json_content_original = None
    query_output_json_content_original = None
    body_value = None
    #
    if os.path.exists(scratch_dir_json_fn):
        analysis_parameters_json_content_original = json.load(open(scratch_dir_json_fn + '/analysis_parameters.json'))
        query_output_json_content_original = json.load(open(scratch_dir_json_fn + '/query_output.json'))
    elif os.path.exists(scratch_dir_json_fn_aliased):
        analysis_parameters_json_content_original = json.load(
            open(scratch_dir_json_fn_aliased + '/analysis_parameters.json'))
        query_output_json_content_original = json.load(open(scratch_dir_json_fn_aliased + '/query_output.json'))

    if analysis_parameters_json_content_original is not None:
        analysis_parameters_json_content_original.pop('token', None)
        analysis_parameters_json_content_original_str = email_helper.wrap_python_code(
            json.dumps(analysis_parameters_json_content_original))
        body_value = 'Body of the article with the analysis_parameters.json: <br/><br/>' \
                     '<div style="background-color: lightgray; display: inline-block; padding: 5px;">' + \
                     analysis_parameters_json_content_original_str.replace("\n", "<br>") + '</div>'
        instrument = analysis_parameters_json_content_original['instrument']
        product_type = analysis_parameters_json_content_original['product_type']
    else:
        raise RequestNotUnderstood(message="Request data ont found",
                                   payload={'error_message': 'error while posting article'})

    body_gallery_node["body"][0]["value"] = body_value

    headers = {
        'Content-type': 'application/hal+json',
        'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MzcyMjAxNDcsImV4cCI6MTYzNzIyMzc0NywiZHJ1cGFsIjp7InVpZCI6IjQifX0.UhCxoRjRiyVX2ILhZRXAineNZ3cmxc-b-fHra00chFE'
    }
    # get taxonomy info for the instrument
    log_res = requests.get("http://cdciweb02.isdc.unige.ch/mmoda-pg/taxonomy/term_name/" + instrument,
                           headers=headers
                           )
    output_post = log_res.json()
    if len(output_post) > 0:
        body_gallery_node['field_instrument'] = [{
            "target_id": output_post[0]['tid']
        }]
    # get taxonomy info for the product
    log_res = requests.get("http://cdciweb02.isdc.unige.ch/mmoda-pg/taxonomy/term_name/" + product_type,
                           headers=headers
                           )
    output_post = log_res.json()
    if len(output_post) > 0:
        body_gallery_node['field_product'] = [{
            "target_id": output_post[0]['tid']
        }]
    # post an article
    log_res = requests.post("http://cdciweb02.isdc.unige.ch/mmoda-pg/node?_format=hal_json",
                            data=json.dumps(body_gallery_node),
                            headers=headers
                            )
    output_post = log_res.json()
    if log_res.status_code < 200 or log_res.status_code >= 300:
        raise RequestNotUnderstood(output_post['message'],
                                   status_code=log_res.status_code,
                                   payload={'error_message': 'error while posting article'})

    return output_post
