import time as time_
import logging
import os
import requests
import glob

from ..analysis import tokenHelper
from ..analysis.exceptions import BadRequest, MissingRequestParameter
from ..flask_app.sentry import sentry

logger = logging.getLogger()

num_msgs_sending_max_tries = 5
msg_sending_retry_sleep_s = .5

class MatrixMsgNotSent(BadRequest):
    pass


def send_incident_report_message(
        config,
        job_id,
        session_id,
        logger,
        decoded_token,
        incident_content=None,
        incident_time=None,
        scratch_dir=None,
        sentry_dsn=None):

    sending_time = time_.time()




def send_job_message(
        config,
        logger,
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
        scratch_dir=None,
        sentry_dsn=None):
    sending_time = time_.time()



def send_message(
        url_server,
        sender_alias, # from config
        room_id, # from config and token
        message_text,
        message_body_html,
        logger
):
    logger.info(f"Sending message to the room id: {room_id}")
    url = f'{url_server}/_matrix/client/r0/rooms/' + room_id + '/send/m.room.message'

    headers = {
        'Authorization': ' '.join(['Bearer', sender_alias]),
        'Content-type': 'application/json'
    }

    data = {
        'body': message_text,
        'format': 'org.matrix.custom.html',
        'formatted_body': message_body_html,
        'msgtype': 'm.text'
    }

    res = requests.post(url, json=data, headers=headers)

    logger.info("Message successfully sent")

    return res


def is_message_to_send_run_query(logger, status, time_original_request, scratch_dir, job_id, config, decoded_token=None):

    log_additional_info_obj = {}
    sending_ok = False
    time_check = time_.time()
    sentry_for_email_sending_check = config.sentry_for_email_sending_check
    # get total request duration
    if decoded_token:
        # in case the job is just submitted and was not submitted before, at least since some time
        logger.info("considering email sending, status: %s, time_original_request: %s", status, time_original_request)

        matrix_message_sending_job_submitted = tokenHelper.get_token_user_submitted_matrix_message(decoded_token)
        info_parameter = 'extracted from token'
        if matrix_message_sending_job_submitted is None:
            # in case this didn't come with the token take the default value from the configuration
            matrix_message_sending_job_submitted = config.matrix_message_sending_job_submitted
            info_parameter = 'extracted from the configuration'

        log_additional_info_obj['matrix_message_sending_job_submitted'] = f'{matrix_message_sending_job_submitted}, {info_parameter}'
        logger.info("matrix_message_sending_job_submitted: %s", matrix_message_sending_job_submitted)

        # get the amount of time passed from when the last email was sent
        interval_ok = True

        matrix_message_sending_job_submitted_interval = tokenHelper.get_token_user_sending_submitted_interval_matrix_message(
            decoded_token)
        info_parameter = 'extracted from token'
        if matrix_message_sending_job_submitted_interval is None:
            # in case this didn't come with the token take the default value from the configuration
            matrix_message_sending_job_submitted_interval = config.matrix_message_sending_job_submitted_default_interval
            info_parameter = 'extracted from the configuration'

        logger.info("matrix_message_sending_job_submitted_interval: %s", matrix_message_sending_job_submitted_interval)
        log_additional_info_obj[
            'matrix_message_sending_job_submitted_interval'] = f'{matrix_message_sending_job_submitted_interval}, {info_parameter}'

        matrix_message_history_dir = os.path.join(scratch_dir, 'matrix_message_history')
        logger.info("matrix_message_history_dir: %s", matrix_message_history_dir)

        matrix_message_history_dirs_same_job_id = f"scratch_*_{job_id}*/matrix_message_history"
        logger.info("matrix_message_history_dirs_same_job_id: %s", matrix_message_history_dirs_same_job_id)

        # find all
        submitted_matrix_message_pattern = os.path.join(
            matrix_message_history_dirs_same_job_id,
            'matrix_message_submitted_*.msg'
        )
        submitted_matrix_message_files = glob.glob(submitted_matrix_message_pattern)
        logger.info("submitted_matrix_message_files: %s as %s", len(submitted_matrix_message_files), submitted_matrix_message_pattern)
        log_additional_info_obj['submitted_matrix_message_files'] = submitted_matrix_message_files

        if len(submitted_matrix_message_files) >= 1:
            times = []
            for f in submitted_matrix_message_files:
                f_name, f_ext = os.path.splitext(os.path.basename(f))
                if f_ext == '.msg' and f_name:
                    times.append(float(f_name.split('_')[2]))

            time_last_matrix_message_submitted_sent = max(times)
            time_from_last_submitted_matrix_message = time_check - float(time_last_matrix_message_submitted_sent)
            interval_ok = time_from_last_submitted_matrix_message > matrix_message_sending_job_submitted_interval

        logger.info("interval_ok: %s", interval_ok)
        log_additional_info_obj['interval_ok'] = interval_ok

        status_ok = True
        if status != 'submitted':
            status_ok = False
            logger.info(f'status {status} not a valid one for sending a message on matrix after a run_query')
            if sentry_for_email_sending_check:
                sentry.capture_message((f'an attempt to send a message on the via matrix for the job {job_id} '
                                        f'has been detected at the completion '
                                        f'of the run_query method with the status: {status}'))

        # send submitted mail, status update
        sending_ok = matrix_message_sending_job_submitted and interval_ok and status_ok
        if sending_ok:
            log_additional_info_obj['check_result_message'] = 'the email will be sent'
            log_matrix_message_sending_info(logger=logger,
                                            status=status,
                                            time_request=time_check,
                                            scratch_dir=scratch_dir,
                                            job_id=job_id,
                                            additional_info_obj=log_additional_info_obj
                                            )
    else:
        logger.info(f'an email will not be sent because a token was not provided')

    return sending_ok


def is_message_to_send_callback(logger, status, time_original_request, scratch_dir, config, job_id, decoded_token=None, sentry_dsn=None):
    log_additional_info_obj = {}
    sending_ok = False
    time_check = time_.time()
    sentry_for_email_sending_check = config.sentry_for_email_sending_check


