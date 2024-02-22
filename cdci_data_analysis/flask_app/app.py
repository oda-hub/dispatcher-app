#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May 10 10:55:20 2017

@author: Andrea Tramcere, Volodymyr Savchenko
"""
import glob
import string
import random
import hashlib
import json
import os
import string
import random
import hashlib
import validators

import logging

from raven.contrib.flask import Sentry
from flask import jsonify, send_from_directory, redirect, Response, Flask, request, make_response, g, url_for

# restx not really used
from flask_restx import Api, Resource

import time as _time
from urllib.parse import urlencode, urlparse

from cdci_data_analysis.analysis import drupal_helper, tokenHelper, renku_helper, email_helper, matrix_helper
from .logstash import logstash_message
from .schemas import QueryOutJSON, dispatcher_strict_validate
from marshmallow.exceptions import ValidationError

from ..plugins import importer

from ..analysis.queries import *
from ..analysis.io_helper import FitsFile
from ..analysis.plot_tools import Image
from .dispatcher_query import InstrumentQueryBackEnd
from ..analysis.exceptions import APIerror, MissingRequestParameter
from ..app_logging import app_logging

from ..analysis.json import CustomJSONEncoder
from .sentry import sentry

from cdci_data_analysis import __version__
import oda_api
from oda_api.api import DispatcherAPI

from cdci_data_analysis.configurer import ConfigEnv

logger = app_logging.getLogger('flask_app')


app = Flask(__name__,
            static_url_path=os.path.abspath('./'),
            static_folder='/static')

sentry.app = app

app.json_encoder = CustomJSONEncoder


api = Api(app=app, version='1.0', title='CDCI ODA API',
          description='API for ODA CDCI dispatcher microservices\n Author: Andrea Tramacere, Volodymyr Savchenko')


ns_conf = api.namespace('api/v1.0/oda', description='api')


@app.before_request
def before_request():
    g.request_start_time = _time.time()

@app.route('/reload-plugin/<name>')
def reload_plugin(name):
    try:
        importer.reload_plugin(name)
        return f'Plugin {name} reloaded\n'
    except ModuleNotFoundError as e:
        return f'Plugin {name} not found\n', 400

    
@app.route("/api/meta-data")
def run_api_meta_data():
    query = InstrumentQueryBackEnd(app, get_meta_data=True)
    return query.get_meta_data()


@app.route("/api/parameters")
def run_api_parameters():
    query = InstrumentQueryBackEnd(app, get_meta_data=True)
    return query.get_paramters_dict()


@app.route("/api/instr-list")
def run_api_instr_list():
    logger.warning('\nThe endpoint \'/api/instr-list\' is deprecated and you will be automatically redirected to the '
                   '\'/instr-list\' endpoint. Please use this one in the future.\n')

    if app.config['conf'].products_url is not None and validators.url(app.config['conf'].products_url):
        redirection_url = os.path.join(app.config['conf'].products_url, 'dispatch-data/instr-list')
        if request.args:
            args_request = urlencode(request.args)
            redirection_url = f'{redirection_url}?{args_request}'

    else:
        parsed_request_url = urlparse(request.url)
        path_request_url = parsed_request_url.path.replace('/api', '')

        parsed_request_url = parsed_request_url._replace(path=path_request_url)
        redirection_url = parsed_request_url.geturl()

    return redirect(redirection_url)


@app.route('/meta-data')
def meta_data():
    query = InstrumentQueryBackEnd(app, get_meta_data=True)
    return query.get_meta_data()


@app.route('/api/par-names')
def get_api_par_names():
    query = InstrumentQueryBackEnd(app, get_meta_data=True)
    return query.get_api_par_names()


@app.route('/check_satus')
def check_satus():
    par_dic = {}
    par_dic['instrument'] = 'mock'
    par_dic['query_status'] = 'new'
    par_dic['session_id'] = u''.join(random.choice(
        string.ascii_uppercase + string.digits) for _ in range(16))
    par_dic['job_status'] = 'submitted'
    query = InstrumentQueryBackEnd(
        app, par_dic=par_dic, get_meta_data=False, verbose=True)
    print('request', request.method)
    return query.run_query_mock()


@app.route('/meta-data-src')
def meta_data_src():
    query = InstrumentQueryBackEnd(app, get_meta_data=True)
    return query.get_meta_data('src_query')


@app.route("/download_products", methods=['POST', 'GET'])
def download_products():
    query = InstrumentQueryBackEnd(app, download_products=True)
    return query.download_products()


class UnknownDispatcherException(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


@app.errorhandler(UnknownDispatcherException)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.errorhandler(APIerror)
def handle_bad_request(error):
    response = jsonify({**error.to_dict(),
                        'error': str(error) + ':' + error.message,
                        **common_exception_payload()})
    response.status_code = error.status_code
    return response


def remove_nested_keys(D, keys):
    if isinstance(D, dict):
        return {k: remove_nested_keys(v, keys) for k, v in D.items() if k not in keys}

    if isinstance(D, list):
        return [remove_nested_keys(v, keys) for v in D]

    if isinstance(D, tuple):
        return tuple([remove_nested_keys(v, keys) for v in D])

    return D


def common_exception_payload():
    payload = {}

    payload['cdci_data_analysis_version'] = __version__
    payload['cdci_data_analysis_version_details'] = os.getenv('DISPATCHER_VERSION_DETAILS', 'unknown')
    payload['oda_api_version'] = oda_api.__version__
     
    _l = []

    for instrument_factory in importer.instrument_factory_list:
        _l.append('%s' % instrument_factory().name)

    payload['installed_instruments'] = _l

    payload['debug_mode'] = os.environ.get(
        'DISPATCHER_DEBUG_MODE', 'no')  # change the default

    # TODO why only in debug_mode ?
    # if payload['debug_mode'] == "yes":

    payload['config'] = {
        'dispatcher-config': remove_nested_keys(app.config['conf'].as_dict(),
                                                ['sentry_url', 'logstash_host', 'logstash_port','secret_key',
                                                 'product_gallery_secret_key',
                                                 'matrix_sender_access_token', 'matrix_incident_report_sender_personal_access_token',
                                                 'matrix_bcc_receivers_room_ids', 'matrix_incident_report_receivers_room_ids',
                                                 'smtp_server_password'])
    }

    plugins = {}
    payload['config']['plugins'] = plugins
    for plugin_name, plugin_module in importer.cdci_plugins_dict.items():
        plugins[plugin_name] = {
            'config_file': plugin_module.conf_file
        }

    return payload


@app.route('/update_token_email_options', methods=['POST', 'GET'])
def update_token_email_options():
    logger.info("request.args: %s ", request.args)

    query = InstrumentQueryBackEnd(app, update_token=True)

    query.update_token(update_email_options=True)
    # TODO adaption to the QueryOutJSON schema is needed
    return query.token


@app.route('/refresh_token', methods=['POST', 'GET'])
def refresh_token():
    logger.info("request.args: %s ", request.args)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['refresh-tokens'],
                                                                  action="refresh your token")

    if output_code is not None:
        return make_response(output, output_code)

    query = InstrumentQueryBackEnd(app, update_token=True)

    query.update_token(refresh_token=True)
    # TODO adaption to the QueryOutJSON schema is needed
    return query.token


@app.route('/free-up-space', methods=['POST', 'GET'])
def free_up_space():
    free_up_result_data_obj = InstrumentQueryBackEnd.free_up_space(app)
    return free_up_result_data_obj


@app.route('/inspect-state', methods=['POST', 'GET'])
def inspect_state():
    token = request.args.get('token', None)

    app_config = app.config.get('conf')
    secret_key = app_config.secret_key
    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['job manager'],
                                                                  action="inspect the state for a given job_id")

    if output_code is not None:
        return make_response(output, output_code)
    state_data_obj = InstrumentQueryBackEnd.inspect_state()
    return jsonify(dict(records=state_data_obj['records']))


@app.route('/instr-list')
def instr_list():
    instrument_list = InstrumentQueryBackEnd.get_user_specific_instrument_list(app)
    return instrument_list


@app.route('/push-renku-branch', methods=['POST'])
def push_renku_branch():
    logger.info("request.args: %s ", request.args)

    try:

        token = request.args.get('token', None)
        app_config = app.config.get('conf')
        secret_key = app_config.secret_key

        output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                      required_roles=['renku contributor'],
                                                                      action="perform this operation")

        if output_code is not None:
            return make_response(output, output_code)
        decoded_token = output

        user_name = tokenHelper.get_token_user(decoded_token)
        user_email = tokenHelper.get_token_user_email_address(decoded_token)

        par_dic = request.values.to_dict()
        par_dic.pop('token')
        # TODO check job_id is provided with the request
        job_id = par_dic.pop('job_id')
        # Get the API code to push to the new renku branch
        scratch_dir_pattern = f'scratch_sid_*_jid_{job_id}*'
        list_scratch_folders = glob.glob(scratch_dir_pattern)
        if len(list_scratch_folders) >= 1:
            analysis_parameters_content_original = None
            for scratch_folder in list_scratch_folders:
                analysis_parameters_path = os.path.join(scratch_folder, 'analysis_parameters.json')
                if os.path.exists(analysis_parameters_path):
                    with open(analysis_parameters_path) as a_par_f:
                        analysis_parameters_content_original = json.load(a_par_f)
                    break
            if analysis_parameters_content_original is None:
                error_message = ("Error while posting data in the renku branch: "
                                 f"analysis_parameters.json file was not found for the given job_id {job_id}")
                raise RequestNotUnderstood(error_message)
            request_dict = analysis_parameters_content_original
            api_code = DispatcherAPI.set_api_code(request_dict, url=os.path.join(app.config['conf'].products_url, 'dispatch-data'))
        else:
            error_message = ("Error while posting data in the renku branch: "
                             f"no scratch folder was found with the given job_id :{job_id}")
            raise RequestNotUnderstood(error_message)

        renku_gitlab_repository_url = app_config.renku_gitlab_repository_url
        renku_gitlab_ssh_key_path = app_config.renku_gitlab_ssh_key_path
        renku_base_project_url = app_config.renku_base_project_url
        products_url = app_config.products_url

        renku_logger = logger.getChild('push_renku_branch')
        renku_logger.info('renku_gitlab_repository_url: %s', renku_gitlab_repository_url)
        renku_logger.info('renku_base_project_url: %s', renku_base_project_url)
        renku_logger.info('renku_gitlab_ssh_key_path: %s', renku_gitlab_ssh_key_path)
        renku_logger.info('user_name: %s', user_name)
        renku_logger.info('user_email: %s', user_email)

        api_code_url = renku_helper.push_api_code(api_code=api_code,
                                                  token=token,
                                                  job_id=job_id,
                                                  renku_gitlab_repository_url=renku_gitlab_repository_url,
                                                  renku_base_project_url=renku_base_project_url,
                                                  renku_gitlab_ssh_key_path=renku_gitlab_ssh_key_path,
                                                  user_name=user_name, user_email=user_email,
                                                  products_url=products_url,
                                                  request_dict=analysis_parameters_content_original)

        return api_code_url

    except Exception as e:
        error_message = f"Exception in push-renku-branch: "
        if hasattr(e, 'message') and e.message is not None:
            error_message += e.message
        else:
            error_message += f"{repr(e)}"
        error_message += f", {traceback.format_exc()}"

        logging.getLogger().error(error_message)

        sentry.capture_message(f'exception while posting on the renku branch: {error_message}')
        
        raise RequestNotUnderstood(message="Internal error while posting on the renku branch. "
                                           "Our team is notified and is working on it.")



@app.route('/run_analysis', methods=['POST', 'GET'])
def run_analysis():
    """
    DRAFT
    ---
    operationId: 'run_analysis'
    parameters:
    - name: 'query_status'
      in: 'query'
      required: false
      type: 'string'
    responses:
      200:
        description: 'analysis done'
        schema:
          $ref: '#/definitions/QueryOutJSON'
      202:
        description: 'request accepted but not done yet'
      400:
        description: 'something in request not understood - missing, unexpected values'
    """

    request_summary = log_run_query_request()

    try:
        logger.info('\033[32m===> dataserver_call_back\033[0m')
        logger.info('\033[33m raw request values: %s \033[0m',
                    dict(request.values))

        query_id = hashlib.sha224(str(request.values).encode()).hexdigest()[:8]

        t0 = g.request_start_time
        query = InstrumentQueryBackEnd(app, query_id=query_id)
        r = query.run_query(disp_conf=app.config['conf'])
        logger.info("run_analysis for %s took %g seconds", request.args.get(
            'client-name', 'unknown'), _time.time() - t0)

        logger.info("towards log_run_query_result")
        log_run_query_result(request_summary, r[0])

        return r

    except APIerror as e:
        raise
    except Exception as e:
        sentry.capture_message(f"exception in run_analysis: {repr(e)} {traceback.format_exc()}")

        raise UnknownDispatcherException('request not valid',
                                         status_code=500,
                                         payload={'error_message': str(e), **common_exception_payload()})


# or flask-marshmellow
@app.after_request
def validate_schema(response):
    try:
        if dispatcher_strict_validate:
            # TODO in case of download/js9 request a dedicated validation schema should be defined
            if not response.is_streamed:
                QueryOutJSON().load(response.json)
    except ValidationError as e:
        logger.error("response not validated: %s; %s", e, json.dumps(response.json, sort_keys=True, indent=4))
        return jsonify({
            'error': repr(e),
            'invalid_response': response.json
        }), 500
    return response


@app.route('/test_mock', methods=['POST', 'GET'])
def test_mock():
    # instrument_name='ISGRI'
    query = InstrumentQueryBackEnd(app)
    return query.run_query_mock()


@app.route('/resolve-job-url', methods=['GET'])
def resolve_job_url():
    try:
        logger.info('\033[32m===========================> resolve_job_url\033[0m')

        query = InstrumentQueryBackEnd(app, instrument_name='mock', resolve_job_url=True)
        location = query.resolve_job_url()

        return redirect(location, 302)
    except APIerror as e:
        logging.getLogger().error("exception in run_analysis: %s %s",
                                  e.message, traceback.format_exc())
        message_dict = {
            "error_message": e.message,
            "status_code": e.status_code
        }
        location = '%s?%s' % (app.config['conf'].products_url, urlencode(message_dict))
        return redirect(location, 302)
    except Exception as e:
        logging.getLogger().error("exception in run_analysis: %s %s",
                                  repr(e), traceback.format_exc())
        message_dict = {
            "error_message": repr(e)
        }
        location = '%s?%s' % (app.config['conf'].products_url, urlencode(message_dict))
        return redirect(location, 302)


@app.route('/call_back', methods=['POST', 'GET'])
def dataserver_call_back():
    logger.info('\033[32m===========================> dataserver_call_back\033[0m')

    logger.info('\033[33m raw request values: %s \033[0m', dict(request.values))

    query_id = hashlib.sha224(str(request.values).encode()).hexdigest()[:8]

    t0 = _time.time()
    # TODO get rid of the mock instrument
    query = InstrumentQueryBackEnd(
        app,
        instrument_name='mock',
        data_server_call_back=True,
        query_id=query_id)
    logger.info(f'\033[32m===========================> [{query_id}] dataserver_call_back constructor done in {_time.time() - t0} s\033[0m')
    query.run_call_back()
    logger.info(f'\033[32m===========================> [{query_id}] dataserver_call_back DONE in {_time.time() - t0}\033[0m')
    return jsonify({'time_spent_s': _time.time() - t0})


####################################### API #######################################

@api.errorhandler(APIerror)
def handle_api_error(error):
    print("=> APIerror flask handler", error)
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

# TODO: apparently flask-restplus modifies (messes up) error handling of flask.
# since it's deprecated and to be removed, no reason to try figuring it out


@api.errorhandler(Exception)
def handle_error(error):
    print("=> APIerror flask handler", error)
    return make_response(f"unmanagable error: {error}"), 400


def output_html(data, code, headers=None):
    resp = Response(data, mimetype='text/html', headers=headers)
    resp.status_code = code
    return resp


@ns_conf.route('/product/<path:path>', methods=['GET', 'POST'])
# @app.route('/product/<path:path>',methods=['GET','POST'])
class Product(Resource):
    # @api.doc(responses={410: 'problem with local file delivery'}, params={'path': 'the file path to be served'})
    def get(self, path):
        """
        serves a locally stored file
        """
        try:
            return send_from_directory(os.path.abspath('./'), path)
        except Exception as e:
            # print('qui',e)
            raise APIerror('problem with local file delivery: %s' %
                           e, status_code=410)


@app.route('/resolve_name', methods=['GET'])
def resolve_name():
    logger.info("request.args: %s ", request.args)
    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="post on the product gallery")

    if output_code is not None:
        return make_response(output, output_code)

    name = request.args.get('name', None)

    name_resolver_url = app_config.name_resolver_url
    entities_portal_url = app_config.entities_portal_url

    resolve_object = drupal_helper.resolve_name(name_resolver_url=name_resolver_url,
                                                entities_portal_url=entities_portal_url,
                                                name=name)

    return resolve_object


@app.route('/get_revnum', methods=['GET'])
def get_revnum():
    logger.info("request.args: %s ", request.args)
    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="post on the product gallery")

    if output_code is not None:
        return make_response(output, output_code)

    time_to_convert = request.args.get('time_to_convert', None)

    converttime_revnum_service_url = app_config.converttime_revnum_service_url

    resolve_object = drupal_helper.get_revnum(service_url=converttime_revnum_service_url, time_to_convert=time_to_convert)

    return resolve_object


@app.route('/get_list_terms', methods=['GET'])
def get_list_terms():
    logger.info("request.args: %s ", request.args)
    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="post on the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    sentry_dsn = sentry.sentry_url

    group = request.args.get('group', None)
    parent = request.args.get('parent', None)

    list_terms = drupal_helper.get_list_terms(disp_conf=app_config,
                                              group=group,
                                              parent=parent,
                                              decoded_token=decoded_token,
                                              sentry_dsn=sentry_dsn)

    output_request = json.dumps(list_terms)

    return output_request


@app.route('/get_parents_term', methods=['GET'])
def get_parents_term():
    logger.info("request.args: %s ", request.args)
    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="post on the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    sentry_dsn = sentry.sentry_url

    group = request.args.get('group', None)
    term = request.args.get('term', None)

    list_parents = drupal_helper.get_parents_term(disp_conf=app_config,
                                                  term=term,
                                                  group=group,
                                                  decoded_token=decoded_token,
                                                  sentry_dsn=sentry_dsn)

    output_request = json.dumps(list_parents)

    return output_request


@app.route('/get_observation_attachments', methods=['GET'])
def get_observation_attachments():
    logger.info("request.args: %s ", request.args)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="post on the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    par_dic = request.values.to_dict()
    par_dic.pop('token')

    sentry_dsn = sentry.sentry_url

    gallery_secret_key = app_config.product_gallery_secret_key
    product_gallery_url = app_config.product_gallery_url
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = drupal_helper.get_user_id(product_gallery_url=product_gallery_url,
                                                        user_email=user_email,
                                                        sentry_dsn=sentry_dsn)
    gallery_jwt_token = drupal_helper.generate_gallery_jwt_token(gallery_secret_key, user_id=user_id_product_creator)

    observation_title = par_dic.pop('title', None)

    output_get = drupal_helper.get_observation_yaml_attachments_by_observation_title(
        product_gallery_url, gallery_jwt_token,
        observation_title=observation_title,
        sentry_dsn=sentry_dsn
        )

    return output_get


@app.route('/get_all_revs', methods=['GET'])
def get_all_revs():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="getting all the revolutions from the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    par_dic = request.values.to_dict()
    par_dic.pop('token')

    sentry_dsn = sentry.sentry_url

    gallery_secret_key = app_config.product_gallery_secret_key
    product_gallery_url = app_config.product_gallery_url
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = drupal_helper.get_user_id(product_gallery_url=product_gallery_url,
                                                        user_email=user_email,
                                                        sentry_dsn=sentry_dsn)
    # update the token
    gallery_jwt_token = drupal_helper.generate_gallery_jwt_token(gallery_secret_key, user_id=user_id_product_creator)

    output_get = drupal_helper.get_all_revolutions(product_gallery_url=product_gallery_url,
                                                   gallery_jwt_token=gallery_jwt_token,
                                                   sentry_dsn=sentry_dsn)

    output_list = json.dumps(output_get)

    return output_list


@app.route('/get_all_astro_entities', methods=['GET'])
def get_all_astro_entities():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="getting all the astro entities from the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    par_dic = request.values.to_dict()
    par_dic.pop('token')

    sentry_dsn = sentry.sentry_url

    gallery_secret_key = app_config.product_gallery_secret_key
    product_gallery_url = app_config.product_gallery_url
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = drupal_helper.get_user_id(product_gallery_url=product_gallery_url,
                                                        user_email=user_email,
                                                        sentry_dsn=sentry_dsn)
    # update the token
    gallery_jwt_token = drupal_helper.generate_gallery_jwt_token(gallery_secret_key, user_id=user_id_product_creator)

    output_get = drupal_helper.get_all_source_astrophysical_entities(product_gallery_url=product_gallery_url,
                                                                     gallery_jwt_token=gallery_jwt_token,
                                                                     sentry_dsn=sentry_dsn)
    output_list = json.dumps(output_get)

    return output_list


@app.route('/get_astro_entity_info_by_source_name', methods=['GET'])
def get_astro_entity_info_by_source_name():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    app_config = app.config.get('conf')

    sentry_dsn = sentry.sentry_url

    product_gallery_url = app_config.product_gallery_url
    src_name = request.args.get('src_name', None)

    source_entity_info = drupal_helper.get_source_astrophysical_entity_info_by_source_and_alternative_name(product_gallery_url,
                                                                                                          gallery_jwt_token=None,
                                                                                                          source_name=src_name,
                                                                                                          sentry_dsn=sentry_dsn)

    refactored_astro_entity_info = {}
    astro_entity_info = {}
    if len(source_entity_info) >= 1:
        astro_entity_info = source_entity_info[0]

    for k, v in astro_entity_info.items():
        refactored_key = k
        if k.startswith('field_'):
            refactored_key = k.replace('field_', '')
        refactored_astro_entity_info[refactored_key] = v
    if 'nid' in astro_entity_info:
        nid =  astro_entity_info['nid']

        refactored_astro_entity_info['url_preview'] = os.path.join(product_gallery_url, f'node/{nid}/preview')

    return refactored_astro_entity_info


@app.route('/get_data_product_list_with_conditions', methods=['GET'])
def get_data_product_list_with_conditions():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    par_dic = request.values.to_dict()
    token = par_dic.pop('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="getting all the astro entities from the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    sentry_dsn = sentry.sentry_url

    gallery_secret_key = app_config.product_gallery_secret_key
    product_gallery_url = app_config.product_gallery_url
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = drupal_helper.get_user_id(product_gallery_url=product_gallery_url,
                                                        user_email=user_email,
                                                        sentry_dsn=sentry_dsn)
    # update the token
    gallery_jwt_token = drupal_helper.generate_gallery_jwt_token(gallery_secret_key, user_id=user_id_product_creator)

    # src_name = par_dic.pop('src_name', None)

    output_get = drupal_helper.get_data_product_list_by_source_name_with_conditions(product_gallery_url=product_gallery_url,
                                                                                    gallery_jwt_token=gallery_jwt_token,
                                                                                    sentry_dsn=sentry_dsn,
                                                                                    **par_dic)
    output_list = json.dumps(output_get)

    return output_list


# TODO to refactor using get_data_product_list_with_conditions
@app.route('/get_data_product_list_by_source_name', methods=['GET'])
def get_data_product_list_by_source_name():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="getting all the astro entities from the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    par_dic = request.values.to_dict()
    par_dic.pop('token')

    sentry_dsn = sentry.sentry_url

    gallery_secret_key = app_config.product_gallery_secret_key
    product_gallery_url = app_config.product_gallery_url
    user_email = tokenHelper.get_token_user_email_address(decoded_token)
    user_id_product_creator = drupal_helper.get_user_id(product_gallery_url=product_gallery_url,
                                                        user_email=user_email,
                                                        sentry_dsn=sentry_dsn)
    # update the token
    gallery_jwt_token = drupal_helper.generate_gallery_jwt_token(gallery_secret_key, user_id=user_id_product_creator)

    src_name = request.args.get('src_name', None)

    output_get = drupal_helper.get_data_product_list_by_source_name(product_gallery_url=product_gallery_url,
                                                                    gallery_jwt_token=gallery_jwt_token,
                                                                    src_name=src_name,
                                                                    sentry_dsn=sentry_dsn)
    output_list = json.dumps(output_get)

    return output_list



@app.route('/post_astro_entity_to_gallery', methods=['POST'])
def post_astro_entity_to_gallery():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="post on the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    par_dic = request.values.to_dict()
    par_dic.pop('token')

    output_post = drupal_helper.post_content_to_gallery(decoded_token=decoded_token,
                                                        content_type="astrophysical_entity",
                                                        disp_conf=app_config,
                                                        files=request.files,
                                                        **par_dic)

    return output_post


@app.route('/post_observation_to_gallery', methods=['POST'])
def post_observation_to_gallery():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="post on the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    par_dic = request.values.to_dict()
    par_dic.pop('token')

    output_post = drupal_helper.post_content_to_gallery(decoded_token=decoded_token,
                                                        content_type="observation",
                                                        disp_conf=app_config,
                                                        files=request.files,
                                                        **par_dic)

    return output_post


@app.route('/post_product_to_gallery', methods=['POST'])
def post_product_to_gallery():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="post on the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    par_dic = request.values.to_dict()
    par_dic.pop('token')

    output_post = drupal_helper.post_content_to_gallery(decoded_token=decoded_token,
                                                        disp_conf=app_config,
                                                        files=request.files,
                                                        **par_dic)

    return output_post


@app.route('/delete_product_to_gallery', methods=['POST'])
def delete_product_to_gallery():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="delete from the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    par_dic = request.values.to_dict()
    par_dic.pop('token')

    output_post = drupal_helper.delete_content_gallery(decoded_token=decoded_token,
                                                       disp_conf=app_config,
                                                       files=request.files,
                                                       **par_dic)

    return output_post


@app.route('/post_revolution_processing_log_to_gallery', methods=['POST'])
def post_revolution_processing_log_to_gallery():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key,
                                                                  required_roles=['gallery contributor'],
                                                                  action="post revolution processing log on the product gallery")

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output

    par_dic = request.values.to_dict()
    par_dic.pop('token')

    output_post = drupal_helper.post_content_to_gallery(decoded_token=decoded_token,
                                                        disp_conf=app_config,
                                                        files=request.files,
                                                        **par_dic)

    return output_post


@app.route('/report_incident', methods=['POST'])
def report_incident():
    logger.info("request.args: %s ", request.args)
    logger.info("request.files: %s ", request.files)

    token = request.args.get('token', None)
    app_config = app.config.get('conf')
    secret_key = app_config.secret_key

    output, output_code = tokenHelper.validate_token_from_request(token=token, secret_key=secret_key)

    if output_code is not None:
        return make_response(output, output_code)
    decoded_token = output
    report_incident_status = {}
    par_dic = request.values.to_dict()
    job_id = par_dic.get('job_id')
    session_id = par_dic.get('session_id')
    scratch_dir = par_dic.get('scratch_dir')
    incident_content = par_dic.get('incident_content')
    incident_time = par_dic.get('incident_time', _time.time())
    try:
        email_helper.send_incident_report_email(
            config=app_config,
            job_id=job_id,
            session_id=session_id,
            logger=logger,
            decoded_token=decoded_token,
            incident_content=incident_content,
            incident_time=incident_time,
            scratch_dir=scratch_dir
        )
        report_incident_status['email_report_status'] = 'incident report email successfully sent'
    except email_helper.EMailNotSent as e:
        report_incident_status['email_report_status'] = 'sending email failed'
        logging.warning(f'email sending failed: {e}')        
        sentry.capture_message(f'sending email failed {e}')

    except MissingRequestParameter as e:
        report_incident_status['email_report_status'] = 'sending email failed'
        logging.warning(f'parameter missing during call back: {e}')

    try:
        res_content = matrix_helper.send_incident_report_message(
            config=app_config,
            job_id=job_id,
            session_id=session_id,
            logger=logger,
            decoded_token=decoded_token,
            incident_content=incident_content,
            incident_time=incident_time,
            scratch_dir=scratch_dir
        )

        matrix_message_report_status = 'incident report message successfully sent via matrix'
        if len(res_content['res_content_incident_reports_failed']) >= 1:
            matrix_message_report_status = 'sending of an incident report message via matrix failed'

        report_incident_status['martix_message_report_status'] = matrix_message_report_status
        report_incident_status['martix_message_report_status_details'] = {
            "res_content": res_content
        }

    except MissingRequestParameter as e:
        report_incident_status['martix_message_report_status'] = 'sending message via matrix failed'
        logging.warning(f'parameter missing during call back: {e}')

    return report_incident_status


@ns_conf.route('/js9/<path:path>', methods=['GET', 'POST'])
# @app.route('/js9/<path:path>',methods=['GET','POST'])
class JS9(Resource):
    # @api.doc(responses={410: 'problem with  js9 library'}, params={'path': 'the file path for the JS9 library'})
    def get(self, path):
        """
        serves the js9 library
        """
        try:
            # would like to use config here, but it's not really loaded here
            js9_path = os.environ.get("DISPATCHER_JS9_STATIC_DIR", "static/js9/")
            logger.info("sending js9 from %s %s", js9_path, path)

            if not os.path.exists(os.path.join(js9_path, path)):
                raise Exception(f"js9 not installed on the server, expected in {js9_path}")

            return send_from_directory(js9_path, path)
        except Exception as e:
            # print('qui',e)
            raise APIerror('problem with local file delivery: %s' %
                           e, status_code=410)


@ns_conf.route('/get_js9_plot')
class GetJS9Plot(Resource):
    # @api.doc(responses={410: 'problem with js9 image generation'}, params={'file_path': 'the file path', 'ext_id': 'extension id'})
    def get(self):
        """
        returns the js9 image display
        """
        # api_parser = reqparse.RequestParser()
        # api_parser.add_argument(
        #     'file_path', required=True, help="the name of the file", type=str)
        # api_parser.add_argument('ext_id', required=False,
        #                         help="extension id", type=int, default=4)

        
        # api_args = api_parser.parse_args()

        file_path = request.args.get('file_path')
        ext_id = int(request.args.get('ext_id', 4))

        try:
            tmp_file = FitsFile(file_path)
            tmp_file.file_path._set_file_path(
                tmp_file.file_path.dir_name, 'js9.fits')

            data = FitsFile(file_path).open()[ext_id]
            print('==>', tmp_file.file_path, ext_id)
            data.writeto(tmp_file.file_path.path, overwrite=True)
        except Exception as e:
            # print('qui',e)
            raise APIerror('problem with input file: %s' % e, status_code=410)

        region_file = request.args.get('region_file', None)
        
        print('file_path, region_file', tmp_file.file_path.path, region_file)
        try:
            img = Image(None, None)
            #print('get_js9_plot path',tmp_file.file_path.path)
            img = img.get_js9_html(
                tmp_file.file_path.path, region_file=region_file)

        except Exception as e:
            # print('qui',e)
            raise APIerror('problem with js9 image generation: %s' %
                           e, status_code=410)

        return output_html(img, 200)


@ns_conf.route('/test_js9')
class TestJS9Plot(Resource):
    """
    tests js9 with a predefined file
    """
    # @api.doc(responses={410: 'problem with js9 image generation'})
    def get(self):
        try:
            img = Image(None, None)
            #print('get_js9_plot path',file_path)
            img = img.get_js9_html('dummy_prods/isgri_query_mosaic.fits')

        except Exception as e:
            # print('qui',e)
            raise APIerror('problem with js9 image generation: %s' %
                           e, status_code=410)

        return output_html(img, 200)


# @app.route('/get_js9_plot', methods=['POST', 'GET'])
# def js9_plot():
#    args = request.args.to_dict()
#    file_path = args['file_path']
#    region_file=None
#    if 'region_file' in args.keys():
#        region_file= args['region_file']
#    print('file_path,region_file',file_path,region_file)
#    img=Image(None,None)
#    #print('get_js9_plot path',file_path)
#    return img.get_js9_html(file_path,region_file=region_file)

# @app.route('/test_js9', methods=['POST', 'GET'])
# def test_js9():
#    img = Image(None,None)
#    # print('get_js9_plot path',file_path)
#    return img.get_js9_html('dummy_prods/isgri_query_mosaic.fits')

def conf_app(conf):
    if isinstance(conf, str):
        logger.info("loading config from %s", conf)
        conf = ConfigEnv.from_conf_file(conf, 
                                        set_by=f'command line {__file__}:{__name__}')

    app.config['conf'] = conf
    if getattr(conf, 'sentry_url', None) is not None:
        sentry = Sentry(app, dsn=conf.sentry_url)
        logger.warning("sentry not used")
    return app

def run_app(conf, debug=False, threaded=False):
    conf_app(conf)

    logger.debug(f"starting flask web server: host:{conf.bind_host}, port: {conf.bind_port}, debug: {debug}, "
                f"threaded: {threaded}")

    app.run(host=conf.bind_host, port=conf.bind_port,
            debug=debug, threaded=threaded)


def log_run_query_request():
    request_summary = {}

    try:
        
        try:
            request_json = request.json
        except:
            request_json = {}

        if request_json is None:
            request_json = {}

        logger.debug("output json request")
        logger.debug("request.args: %s", request.args)
        logger.debug("request.host: %s", request.host)
        request_summary['dispatcher-state'] = 'requested'
        request_summary = {'origin': 'dispatcher-run-analysis',
                     'request-data': {
                        'headers': dict(request.headers),
                        'host_url': request.host_url,
                        'host': request.host,
                        'args': dict(request.args),
                        'json-data': dict(request_json),
                        'form-data': dict(request.form or {}),
                        'raw-data': dict(request.data or ""),
                    }}

        try:
            request_summary['clientip'] = request_summary['request-data']['headers']['X-Forwarded-For'].split(",")[0]
            logger.info("extracted client: %s", request_summary['clientip'] )
        except Exception as e:
            logger.warning("unable to extract client")

        request_summary_json = json.dumps(request_summary)
        logger.info("request_summary: %s", request_summary_json)
        logstash_message(app, request_summary_json)
    except Exception as e:
        logger.error("failed to logstash request in log_run_query_request: %s\n%s", repr(e), traceback.format_exception())
        raise

    return request_summary
    

def log_run_query_result(request_summary, result):
    logger.info("IN log_run_query_result")
    try:
        request_summary['dispatcher-state'] = 'returning'

        logger.info("returning data %s", result.data[:100])

        try:
            result_json = json.loads(result.data)
            logger.debug("query result keys: %s", result_json.keys())
            request_summary['return_exit_status']=result_json['exit_status']
            request_summary['return_job_status']=result_json['job_status']
        except Exception:
            logger.warning("not returning json")

        request_summary_json = json.dumps(request_summary)
        logger.debug("request_summary: %s", request_summary_json)
        logstash_message(app, request_summary_json)
    except Exception as e:
        logger.warning("failed to output request %s", e)
        raise

    return result


if __name__ == "__main__":
    app.run()
