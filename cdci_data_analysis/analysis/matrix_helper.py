import time as time_
import os
import requests
import glob
import json
import re
import typing

from ..analysis import tokenHelper
from ..analysis.email_helper import humanize_age, humanize_future
from ..analysis.exceptions import BadRequest, MissingRequestParameter
from ..analysis.hash import make_hash
from ..analysis.time_helper import validate_time
from ..flask_app.sentry import sentry
from ..app_logging import app_logging

from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from bs4 import BeautifulSoup
from urllib import parse

matrix_helper_logger = app_logging.getLogger('matrix_helper')

num_msgs_sending_max_tries = 5
msg_sending_retry_sleep_s = .5

class MatrixMessageNotSent(BadRequest):
    pass


class MultipleDoneMatrixMessage(BadRequest):
    pass

def timestamp2isot(timestamp_or_string: typing.Union[str, float]):
    try:
        timestamp_or_string = validate_time(timestamp_or_string).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OverflowError, TypeError, OSError) as e:
        matrix_helper_logger.warning(f'Error when constructing the datetime object from the timestamp {timestamp_or_string}:\n{e}')
        raise MatrixMessageNotSent(f"Matrix message not sent: {e}")

    return timestamp_or_string


def textify_matrix_message(html):
    html = re.sub('<a href=(.*?)>(.*?)</a>', r'\2: \1', html)

    soup = BeautifulSoup(html)

    for elem in soup.find_all(["a", "p", "div", "h3", "br"]):
        elem.replace_with(elem.text + "\n\n")

    return soup.get_text()



def get_first_submitted_matrix_message_time(scratch_dir):
    first_submitted_matrix_message_time = None
    submitted_matrix_message_pattern = os.path.join(
        scratch_dir,
        'matrix_message_history',
        'matrix_message_submitted_*.json'
    )
    submitted_matrix_message_files = sorted(glob.glob(submitted_matrix_message_pattern), key=os.path.getmtime)
    matrix_helper_logger.info(f"get_first_submitted_matrix_message_time, files: {submitted_matrix_message_files}")
    if len(submitted_matrix_message_files) >= 1:
        f_name, f_ext = os.path.splitext(os.path.basename(submitted_matrix_message_files[0]))
        f_name_split = f_name.split('_')
        if len(f_name_split) == 5:
            try:
                validate_time(f_name_split[4])
                first_submitted_matrix_message_time = float(f_name_split[4])
            except (ValueError, OverflowError, TypeError, OSError) as e:
                matrix_helper_logger.warning(f'Error when extracting the time of the first message submitted via matrix.'
                               f'The value extracted {first_submitted_matrix_message_time} raised the following error:\n{e}')
                first_submitted_matrix_message_time = None
                sentry.capture_message(f'Error when extracting the time of the first message submitted via matrix.'
                               f'The value extracted {first_submitted_matrix_message_time} raised the following error:\n{e}')
        else:
            matrix_helper_logger.warning(f'Error when extracting the time of the first message submitted via matrix: '
                           f'the name of the message file has been found not properly formatted, therefore, '
                           f'the time of the first message submitted via matrix could not be extracted.')
            first_submitted_matrix_message_time = None
            sentry.capture_message(f'Error when extracting the time of the first message submitted via matrix: '
                           f'the name of the message file has been found not properly formatted, therefore, '
                           f'the time of the first message submitted via matrix could not be extracted.')

    return first_submitted_matrix_message_time


def send_incident_report_message(
        config,
        job_id,
        session_id,
        decoded_token,
        incident_content=None,
        incident_time=None,
        scratch_dir=None):

    sending_time = time_.time()

    env = Environment(loader=FileSystemLoader('%s/../flask_app/templates/' % os.path.dirname(__file__)))
    env.filters['timestamp2isot'] = timestamp2isot
    env.filters['humanize_age'] = humanize_age
    env.filters['humanize_future'] = humanize_future

    matrix_server_url = config.matrix_server_url

    incident_report_receivers_room_ids = config.matrix_incident_report_receivers_room_ids
    incident_report_sender_personal_access_token = config.matrix_incident_report_sender_personal_access_token

    matrix_message_data = {
        'request': {
            'job_id': job_id,
            'session_id': session_id,
            'incident_time': incident_time,
            'decoded_token': decoded_token,
        },
        'content': incident_content
    }

    template = env.get_template('incident_report_matrix_message.html')
    message_body_html = template.render(**matrix_message_data)
    message_text = textify_matrix_message(message_body_html)

    # TODO to understand about the line length limit in matrix (if there is any)
    # if invalid_email_line_length(email_text) or invalid_email_line_length(email_body_html):
    #     open("debug_email_lines_too_long.html", "w").write(email_body_html)
    #     open("debug_email_lines_too_long.text", "w").write(email_text)
    #     raise MatrixMessageNotSent(f"message not sent on matrix, lines too long!")
    res_content = {
        'res_content_incident_reports': []
    }

    message_data = {
        'message_data_incident_reports': []
    }

    for incident_report_receiver_room_id in incident_report_receivers_room_ids:
        res_data_message_receiver = send_message(url_server=matrix_server_url,
                                                sender_access_token=incident_report_sender_personal_access_token,
                                                room_id=incident_report_receiver_room_id,
                                                message_text=message_text,
                                                message_body_html=message_body_html
                                                )
        message_data['message_data_incident_reports'].append(res_data_message_receiver['message_data'])
        res_content['res_content_incident_reports'].append(res_data_message_receiver['res_content'])

    store_incident_report_matrix_message(message_data, scratch_dir, sending_time=sending_time)

    return res_content


def send_job_message(
        config,
        decoded_token,
        token,
        job_id,
        session_id,
        status="done",
        status_details=None,
        instrument="",
        product_type="",
        time_request=None,
        request_url="",
        api_code="",
        scratch_dir=None):
    sending_time = time_.time()

    status_details_message = None

    if status_details is not None and status_details['status'] != 'successful':
        if status_details['status'] == 'empty_product' or status_details['status'] == 'empty_result':
            status_details_message = '''Unfortunately, after a quick automated assessment of the request, it has been found that it contains an <b>empty product</b>.
    To the best of our knowledge, no unexpected errors occurred during processing,
    and if this is not what you expected, you probably need to modify the request parameters.<br>'''
        else:
            sentry.capture_message(f'unexpected status_details content before sending a message on matrix: {status_details}')
            matrix_helper_logger.warning(f'unexpected status_details content before sending a message on matrix: {status_details}')
            raise NotImplementedError

    # TODO to be adapted depending on the restrictions of the matrix platform
    if len(request_url) > 2000:
        possibly_compressed_request_url = ""
        permanent_url = False
    elif 2000 > len(request_url) > 600:
        possibly_compressed_request_url = \
            config.products_url + \
            "/dispatch-data/resolve-job-url?" + \
            parse.urlencode(dict(job_id=job_id, session_id=session_id, token=token))
        permanent_url = False
    else:
        possibly_compressed_request_url = request_url
        permanent_url = True

    matrix_server_url = config.matrix_server_url
    matrix_sender_access_token = config.matrix_sender_access_token
    receiver_room_id = tokenHelper.get_token_user_matrix_room_id(decoded_token)

    bcc_receivers_room_ids = config.matrix_bcc_receivers_room_ids

    matrix_message_data = {
        'oda_site': {
            'site_name': config.site_name,
            'frontend_url': config.products_url,
            'contact': config.contact_email_address,
            'manual_reference': config.manual_reference,
        },
        'request': {
            'job_id': job_id,
            'status': status,
            'status_details_message': status_details_message,
            'instrument': instrument,
            'product_type': product_type,
            'time_request': time_request,
            'request_url': possibly_compressed_request_url,
            'api_code': api_code,
            'decoded_token': decoded_token,
            'permanent_url': permanent_url,
        }
    }

    env = Environment(loader=FileSystemLoader(f'{os.path.dirname(__file__)}/../flask_app/templates/'))

    env.filters['timestamp2isot'] = timestamp2isot
    env.filters['humanize_age'] = humanize_age
    env.filters['humanize_future'] = humanize_future

    template = env.get_template('matrix_message.html')
    message_body_html = template.render(**matrix_message_data)
    message_text = textify_matrix_message(message_body_html)
    res_content = {
        'res_content_bcc_users': []
    }

    message_data = {
        'message_data_bcc_users': []
    }
    if receiver_room_id is not None and receiver_room_id != "":
        res_data_message_token_user = send_message(url_server=matrix_server_url,
                                                   sender_access_token=matrix_sender_access_token,
                                                   room_id=receiver_room_id,
                                                   message_text=message_text,
                                                   message_body_html=message_body_html
                                                   )
        message_data_token_user = res_data_message_token_user['message_data']
        res_content_token_user = res_data_message_token_user['res_content']
        message_data['message_data_token_user'] = message_data_token_user
        res_content['res_content_token_user'] = res_content_token_user
    else:
        matrix_helper_logger.warning('a matrix message could not be sent to the token user as no personal room id was '
                                     'provided within the token')

    for bcc_receiver_room_id in bcc_receivers_room_ids:
        if bcc_receiver_room_id is not None and bcc_receiver_room_id != "":
            res_data_message_cc_user = send_message(url_server=matrix_server_url,
                                                    sender_access_token=matrix_sender_access_token,
                                                    room_id=bcc_receiver_room_id,
                                                    message_text=message_text,
                                                    message_body_html=message_body_html
                                                    )
            message_data_cc_user = res_data_message_cc_user['message_data']
            message_data['message_data_bcc_users'].append(message_data_cc_user)
            res_content_cc_user = res_data_message_cc_user['res_content']
            res_content['res_content_bcc_users'].append(res_content_cc_user)


    store_status_matrix_message_info(message_data, status, scratch_dir, sending_time=sending_time, first_submitted_time=time_request)

    return res_content


def send_message(
        url_server=None,
        sender_access_token=None,
        room_id=None,
        message_text=None,
        message_body_html=None,
):
    matrix_helper_logger.info(f"Sending message to the room id: {room_id}")
    url = os.path.join(url_server, f'_matrix/client/r0/rooms/{room_id}/send/m.room.message')

    headers = {
        'Authorization': ' '.join(['Bearer', sender_access_token]),
        'Content-type': 'application/json'
    }

    message_data = {
        'body': message_text,
        'format': 'org.matrix.custom.html',
        'formatted_body': message_body_html,
        'msgtype': 'm.text'
    }

    res = requests.post(url, json=message_data, headers=headers)

    if res.status_code not in [200, 201, 204]:
        try:
            response_json = res.json()
            error_msg = response_json['error']
        except json.decoder.JSONDecodeError:
            error_msg = res.text
        matrix_helper_logger.warning(f"there seems to be some problem in sending a message via matrix:\n"
                                     f"the requested url {url} lead to the error {error_msg}, "
                                     "this might be due to an error in the url or the page requested no longer exists, "
                                     "please check it and try to issue again the request")
        matrix_error_message = error_msg

        sentry.capture_message(f'issue in sending a message via matrix, the requested url {url} lead to the error '
                               f'{matrix_error_message}')
        raise MatrixMessageNotSent('issue in sending a message via matrix',
                                   status_code=res.status_code,
                                   payload={'matrix_error_message': matrix_error_message})

    res_data = {
        "res_content": res.json(),
        "message_data": message_data
    }

    matrix_helper_logger.info("Message successfully sent")

    return res_data


def is_matrix_config_ok(config):
    if config.matrix_server_url is None:
        matrix_helper_logger.info('matrix url server not available')
        return False
    if config.matrix_sender_access_token is None:
        matrix_helper_logger.info('matrix sender_access_token not available')
        return False
    return True


def is_message_to_send_run_query(status, time_original_request, scratch_dir, job_id, config, decoded_token=None):

    log_additional_info_obj = {}
    sending_ok = False
    config_ok = is_matrix_config_ok(config)
    time_check = time_.time()
    sentry_for_matrix_message_sending_check = config.sentry_for_matrix_message_sending_check

    if config.matrix_server_url is None:
        matrix_helper_logger.info('matrix url server not available')
        config_ok = False
    if config.matrix_sender_access_token is None:
        matrix_helper_logger.info('matrix sender_access_token not available')
        config_ok = False

    # get total request duration
    if decoded_token:
        # in case the job is just submitted and was not submitted before, at least since some time
        matrix_helper_logger.info("considering sending a message on matrix, status: %s, time_original_request: %s", status, time_original_request)

        matrix_message_sending_job_submitted = tokenHelper.get_token_user_submitted_matrix_message(decoded_token)
        info_parameter = 'extracted from token'
        if matrix_message_sending_job_submitted is None:
            # in case this didn't come with the token take the default value from the configuration
            matrix_message_sending_job_submitted = config.matrix_message_sending_job_submitted
            info_parameter = 'extracted from the configuration'

        log_additional_info_obj['matrix_message_sending_job_submitted'] = f'{matrix_message_sending_job_submitted}, {info_parameter}'
        matrix_helper_logger.info("matrix_message_sending_job_submitted: %s", matrix_message_sending_job_submitted)

        # get the amount of time passed from when the last message on matrix was sent
        interval_ok = True

        matrix_message_sending_job_submitted_interval = tokenHelper.get_token_user_sending_submitted_interval_matrix_message(
            decoded_token)
        info_parameter = 'extracted from token'
        if matrix_message_sending_job_submitted_interval is None:
            # in case this didn't come with the token take the default value from the configuration
            matrix_message_sending_job_submitted_interval = config.matrix_message_sending_job_submitted_default_interval
            info_parameter = 'extracted from the configuration'

        matrix_helper_logger.info("matrix_message_sending_job_submitted_interval: %s", matrix_message_sending_job_submitted_interval)
        log_additional_info_obj[
            'matrix_message_sending_job_submitted_interval'] = f'{matrix_message_sending_job_submitted_interval}, {info_parameter}'

        matrix_message_history_dir = os.path.join(scratch_dir, 'matrix_message_history')
        matrix_helper_logger.info("matrix_message_history_dir: %s", matrix_message_history_dir)

        matrix_message_history_dirs_same_job_id = f"scratch_*_{job_id}*/matrix_message_history"
        matrix_helper_logger.info("matrix_message_history_dirs_same_job_id: %s", matrix_message_history_dirs_same_job_id)

        # find all
        submitted_matrix_message_pattern = os.path.join(
            matrix_message_history_dirs_same_job_id,
            'matrix_message_submitted_*.json'
        )
        submitted_matrix_message_files = glob.glob(submitted_matrix_message_pattern)
        matrix_helper_logger.info("submitted_matrix_message_files: %s as %s", len(submitted_matrix_message_files), submitted_matrix_message_files)
        log_additional_info_obj['submitted_matrix_message_files'] = submitted_matrix_message_files

        if len(submitted_matrix_message_files) >= 1:
            times = []
            for f in submitted_matrix_message_files:
                f_name, f_ext = os.path.splitext(os.path.basename(f))
                if f_ext == '.json' and f_name:
                    times.append(float(f_name.split('_')[3]))

            time_last_matrix_message_submitted_sent = max(times)
            time_from_last_submitted_matrix_message = time_check - float(time_last_matrix_message_submitted_sent)
            interval_ok = time_from_last_submitted_matrix_message > matrix_message_sending_job_submitted_interval

        matrix_helper_logger.info("interval_ok: %s", interval_ok)
        log_additional_info_obj['interval_ok'] = interval_ok

        status_ok = True
        if status != 'submitted':
            status_ok = False
            matrix_helper_logger.info(f'status {status} not a valid one for sending a message on matrix after a run_query')
            if sentry_for_matrix_message_sending_check:
                sentry.capture_message((f'an attempt to send a message on the via matrix for the job {job_id} '
                                        f'has been detected at the completion '
                                        f'of the run_query method with the status: {status}'))

        # send submitted mail, status update
        sending_ok = matrix_message_sending_job_submitted and interval_ok and status_ok
        if sending_ok:
            log_additional_info_obj['check_result_message'] = 'the message will be sent via matrix'
            log_matrix_message_sending_info(status=status,
                                            time_request=time_check,
                                            scratch_dir=scratch_dir,
                                            job_id=job_id,
                                            additional_info_obj=log_additional_info_obj
                                            )
    else:
        matrix_helper_logger.info(f'a message on matrix will not be sent because a token was not provided')

    return sending_ok and config_ok


def is_matrix_config_present(config):
    url_server = config.matrix_server_url
    sender_access_token = config.matrix_sender_access_token

    if url_server is None or sender_access_token is None:
        matrix_helper_logger.info('matrix url server not available')
        return False

    return True


def is_message_to_send_callback(status, time_original_request, scratch_dir, config, job_id, decoded_token=None):
    log_additional_info_obj = {}
    sending_ok = False
    config_ok = is_matrix_config_ok(config)
    time_check = time_.time()
    sentry_for_matrix_message_sending_check = config.sentry_for_matrix_message_sending_check

    if decoded_token:
        # in case the request was long and 'done'
        matrix_helper_logger.info(f"considering sending a message on matrix, status: {status}, time_original_request: {time_original_request}")

        if status == 'done':
            # get total request duration
            if time_original_request:
                duration_query = time_check - float(time_original_request)
                log_additional_info_obj['query_duration'] = duration_query
            else:
                matrix_helper_logger.info(f'time_original_request not available')
                raise MissingRequestParameter('original request time not available')

            timeout_threshold_matrix_message = tokenHelper.get_token_user_timeout_threshold_matrix_message(decoded_token)
            info_parameter = 'extracted from token'
            if timeout_threshold_matrix_message is None:
                # set it to the default value, from the configuration
                timeout_threshold_matrix_message = config.matrix_message_sending_timeout_default_threshold
                info_parameter = 'extracted from the configuration'

            log_additional_info_obj['timeout_threshold_matrix_message'] = f'{timeout_threshold_matrix_message}, {info_parameter}'
            matrix_helper_logger.info(f"timeout_threshold_matrix_message: {timeout_threshold_matrix_message}")

            matrix_message_sending_timeout = tokenHelper.get_token_user_sending_timeout_matrix_message(decoded_token)
            info_parameter = 'extracted from token'
            if matrix_message_sending_timeout is None:
                matrix_message_sending_timeout = config.matrix_message_sending_timeout
                info_parameter = 'extracted from the configuration'

            log_additional_info_obj['matrix_message_sending_timeout'] = f'{matrix_message_sending_timeout}, {info_parameter}'
            matrix_helper_logger.info("matrix_message_sending_timeout: %s", matrix_message_sending_timeout)

            matrix_helper_logger.info("duration_query > timeout_threshold_matrix_message %s", duration_query > timeout_threshold_matrix_message)
            matrix_helper_logger.info("matrix_message_sending_timeout and duration_query > timeout_threshold_matrix_message %s",
                        matrix_message_sending_timeout and duration_query > timeout_threshold_matrix_message)

            done_matrix_message_files = glob.glob(f'scratch_*_jid_{job_id}*/matrix_message_history/matrix_message_done_*')
            log_additional_info_obj['done_matrix_message_files'] = done_matrix_message_files
            if len(done_matrix_message_files) >= 1:
                matrix_helper_logger.info("the message cannot be sent via matrix because the number of done messages sent is too high: %s", len(done_matrix_message_files))
                raise MultipleDoneMatrixMessage("multiple completion matrix messages detected")

            sending_ok = tokenHelper.get_token_user_done_matrix_message(decoded_token) and matrix_message_sending_timeout and \
                         duration_query > timeout_threshold_matrix_message

        # or if failed
        elif status == 'failed':
            matrix_message_sending_failed = tokenHelper.get_token_user_fail_matrix_message(decoded_token)
            matrix_helper_logger.info("matrix_message_sending_failed: %s", matrix_message_sending_failed)
            log_additional_info_obj['matrix_message_sending_failed'] = matrix_message_sending_failed
            sending_ok = matrix_message_sending_failed

        # not valid status
        else:
            matrix_helper_logger.info(f'status {status} not a valid one for sending a message via matrix after a callback')
            if sentry_for_matrix_message_sending_check:
                sentry.capture_message((f'an attempt in sending a message using matrix has been detected at the completion '
                                        f'of the run_query method with the status: {status}'))
    else:
        matrix_helper_logger.info(f'a message via matrix will not be sent because a token was not provided')

    if sending_ok:
        log_additional_info_obj['check_result_message'] = 'the message will be sent via matrix'
        matrix_helper_logger.info(f"the message will be sent via matrix with a status: {status}")
        log_matrix_message_sending_info(status=status,
                                        time_request=time_check,
                                        scratch_dir=scratch_dir,
                                        job_id=job_id,
                                        additional_info_obj=log_additional_info_obj
                                        )

    return sending_ok and config_ok


def log_matrix_message_sending_info(status, time_request, scratch_dir, job_id, additional_info_obj=None):
    matrix_message_history_folder = os.path.join(scratch_dir, 'matrix_message_history')
    if not os.path.exists(matrix_message_history_folder):
        os.makedirs(matrix_message_history_folder)

    try:
        time_request_str = validate_time(time_request).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OverflowError, TypeError, OSError) as e:
        matrix_helper_logger.warning(f'Error when extracting logging the sending info of a message on matrix.'
                       f'The time value {time_request} raised the following error:\n{e}')
        time_request_str = datetime.fromtimestamp(time_.time()).strftime("%Y-%m-%d %H:%M:%S")
        sentry.capture_message(f'Error when extracting logging the sending info of a message on matrix.'
                               f'The time value {time_request} raised the following error:\n{e}')

    history_info_obj = dict(time=time_request_str,
                            status=status,
                            job_id=job_id)
    if additional_info_obj is not None:
        history_info_obj['additional_information'] = additional_info_obj
    history_info_obj_hash = make_hash(history_info_obj)
    matrix_message_history_log_fn = os.path.join(matrix_message_history_folder, f'matrix_message_history_log_{status}_{time_request}_{history_info_obj_hash}.log')
    with open(matrix_message_history_log_fn, 'w') as outfile:
        outfile.write(json.dumps(history_info_obj, indent=4))

    matrix_helper_logger.info(f"logging matrix message for job id {job_id} sending attempt into {matrix_message_history_log_fn} file")


def store_status_matrix_message_info(message, status, scratch_dir, sending_time=None, first_submitted_time=None):
    current_time = time_.time()
    matrix_message_history_folder = os.path.join(scratch_dir, 'matrix_message_history')
    if not os.path.exists(matrix_message_history_folder):
        os.makedirs(matrix_message_history_folder)

    if sending_time is None:
        sending_time = current_time
    else:
        try:
            validate_time(sending_time)
        except (ValueError, OverflowError, TypeError, OSError) as e:
            matrix_helper_logger.warning(f'Error when writing the content of a message meant to be sent over matrix on a file,'
                           f' the sending time is not valid.'
                           f'The value {sending_time} raised the following error:\n{e}')
            sending_time = current_time
            sentry.capture_message(f'Error when writing the content of a message meant to be sent over matrix on a file,'
                                   f' the sending time is not valid.'
                                   f'The value {sending_time} raised the following error:\n{e}')

    if first_submitted_time is None:
        first_submitted_time = sending_time
    else:
        try:
            validate_time(first_submitted_time)
        except (ValueError, OverflowError, TypeError, OSError) as e:
            matrix_helper_logger.warning(f'Error when writing  the content of a message meant to be sent over matrix,'
                           f' the first submitted time is not valid.'
                           f'The value {first_submitted_time} raised the following error:\n{e}')
            first_submitted_time = sending_time
            sentry.capture_message(f'Error when writing the content of a message meant to be sent over matrix,'
                                   f' the first submitted time is not valid.'
                                   f'The value {first_submitted_time} raised the following error:\n{e}')

    matrix_message_file_name = f'matrix_message_{status}_{str(sending_time)}_{str(first_submitted_time)}.json'

    # record the matrix_message just sent in a dedicated file
    with open(os.path.join(matrix_message_history_folder, matrix_message_file_name), 'w+') as outfile:
        outfile.write(json.dumps(message, indent=4))


def store_incident_report_matrix_message(message, scratch_dir, sending_time=None):
    matrix_message_history_folder_path = os.path.join(scratch_dir, 'matrix_message_history')
    if not os.path.exists(matrix_message_history_folder_path):
        os.makedirs(matrix_message_history_folder_path)
    if sending_time is None:
        sending_time = time_.time()
    # record the message just sent via matrix in a dedicated file
    with open(os.path.join(matrix_message_history_folder_path, 'indident_report_email_' + str(sending_time) + '.json'), 'w+') as outfile:
        outfile.write(json.dumps(message, indent=4))
