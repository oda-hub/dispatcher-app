import time as time_
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from collections import OrderedDict
from urllib.parse import urlencode
import typing

from ..flask_app.sentry import sentry

from ..analysis import tokenHelper
import smtplib
import ssl
import os
import re
import time
import glob
import black
import base64
import logging
from urllib import parse
import zlib
import json
from jinja2 import Environment, FileSystemLoader
from bs4 import BeautifulSoup


from ..analysis.exceptions import BadRequest, MissingRequestParameter
from ..analysis.hash import make_hash

from datetime import datetime

logger = logging.getLogger()

num_email_sending_max_tries = 5
email_sending_retry_sleep_s = .5


class MultipleDoneEmail(BadRequest):
    pass

class EMailNotSent(BadRequest):
    pass

def timestamp2isot(timestamp_or_string: typing.Union[str, float]):
    try:
        timestamp_or_string = float(timestamp_or_string)
    except ValueError:
        pass

    if isinstance(timestamp_or_string, float):
        return datetime.fromtimestamp(float(timestamp_or_string)).strftime("%Y-%m-%d %H:%M:%S")

    return timestamp_or_string

def humanize_interval(time_interval_s: float):
    if time_interval_s < 120:
        return f"{time_interval_s:.1f} seconds"
    elif time_interval_s/60 < 120:
        return f"{time_interval_s/60:.1f} minutes"
    else:
        return f"{time_interval_s/60/60:.1f} hours"


def humanize_age(timestamp: float):
    return humanize_interval(time_.time() - float(timestamp))


def humanize_future(timestamp: float):
    return humanize_interval(float(timestamp) - time_.time())


def textify_email(html):
    html = re.sub('<title>.*?</title>', '', html)
    html = re.sub('<a href=(.*?)>(.*?)</a>', r'\2: \1', html)

    soup = BeautifulSoup(html)

    for elem in soup.find_all(["a", "p", "div", "h3", "br"]):
        elem.replace_with(elem.text + "\n\n")

    return soup.get_text()


def invalid_email_line_length(body):
    for line in body.split('\n'):
        if len(line) > 999:
            return True
    return False

# TODO: not currently fully used, not critical, but should finish this too since it will make nice short permanent urls
def compress_request_url_params(request_url, consider_args=['selected_catalog', 'string_like_name']):
    parsed_url = parse.urlparse(request_url)

    parsed_qs = parse.parse_qs(parsed_url.query)

    compressed_qs = {}
    for k, v in parsed_qs.items():
        if k in consider_args:
            v_json = json.dumps(v)

            if len(v_json) > 100:
                v = "z:" + base64.b64encode(zlib.compress(v_json.encode())).decode()
                logger.info("compressing long %.50s...", v_json)
                logger.info("compressed into %.500s...", v)

        compressed_qs[k] = v


    return parse.urlunparse(parsed_url.__class__(**{
        **parsed_url._asdict(),
        'query': parse.urlencode(compressed_qs)
    }))


# TODO make sure that the list of parameters to ignore in the frontend is synchronized
def generate_products_url_from_par_dict(products_url, par_dict) -> str:
    par_dict = par_dict.copy()

    if 'scw_list' in par_dict and type(par_dict['scw_list']) == list:
        # setting proper scw_list formatting
        # as comma-separated string for being properly read by the frontend
        par_dict['scw_list'] = ",".join(par_dict['scw_list'])

    _skip_list_ = ['token', 'session_id', 'job_id']

    for key, value in dict(par_dict).items():
        if key in _skip_list_ or value is None:
            par_dict.pop(key)

    par_dict = OrderedDict({
        k: par_dict[k] for k in sorted(par_dict.keys())
    })

    request_url = '%s?%s' % (products_url, urlencode(par_dict))
    return request_url

def wrap_python_code(code, max_length=100, max_str_length=None):

    # this black currently does not split strings without spaces

    if max_str_length is None:
        max_str_length = max_length - 10

    while True:
        new_code = code
        for string_separator in '"', "'":
            new_code = re.sub('(%s[0-9a-zA-Z\.\-\/\+,]{%i,}?%s)' % (string_separator, max_str_length + 1, string_separator),
                            lambda S: S.group(1)[:max_str_length] + string_separator + ' ' + string_separator + S.group(1)[max_str_length:],
                            new_code)

        if new_code == code:
            break
        else:
            code = new_code

    logger.debug("\033[31mwrapped: %s\033[0m", code)

    mode = black.Mode(
        target_versions={black.TargetVersion.PY38},
        line_length=max_length,
        string_normalization=True,
        experimental_string_processing=True,
    )

    # this will also ensure it's valid code
    return black.format_str(code, mode=mode)


def check_scw_list_length(
        scw_list
):
    # TODO could be configurable
    if len(scw_list) < 450:
        # still manageable by the proxy
        return True
    else:
        return False


def get_first_submitted_email_time(job_id, scratch_dir):
    first_submitted_email_time = None
    submitted_email_pattern = os.path.join(
        scratch_dir,
        'email_history',
        'email_submitted_*.email'
    )
    submitted_email_files = sorted(glob.glob(submitted_email_pattern), key=os.path.getmtime)

    if len(submitted_email_files) >= 1:
        f_name, f_ext = os.path.splitext(os.path.basename(submitted_email_files[0]))
        first_submitted_email_time = float(f_name.split('_')[3])

    return first_submitted_email_time


def send_incident_report_email(
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

    env = Environment(loader=FileSystemLoader('%s/../flask_app/templates/' % os.path.dirname(__file__)))
    env.filters['timestamp2isot'] = timestamp2isot
    env.filters['humanize_age'] = humanize_age
    env.filters['humanize_future'] = humanize_future

    email_data = {
        'request': {
            'job_id': job_id,
            'session_id': session_id,
            'incident_time': incident_time,
            'decoded_token': decoded_token,
        },
        'content': incident_content
    }

    template = env.get_template('incident_report_email.html')
    email_body_html = template.render(**email_data)

    email_subject = re.search("<title>(.*?)</title>", email_body_html).group(1)
    email_text = textify_email(email_body_html)

    if invalid_email_line_length(email_text) or invalid_email_line_length(email_body_html):
        open("debug_email_lines_too_long.html", "w").write(email_body_html)
        open("debug_email_lines_too_long.text", "w").write(email_text)
        raise EMailNotSent(f"email not sent, lines too long!")

    message = send_email(smtp_server=config.smtp_server,
                         smtp_port=config.smtp_port,
                         sender_email_address=config.incident_report_sender_email_address,
                         cc_receivers_email_addresses=None,
                         bcc_receivers_email_addresses=None,
                         receiver_email_addresses=config.incident_report_receivers_email_addresses,
                         reply_to_email_address=None,
                         email_subject=email_subject,
                         email_text=email_text,
                         email_body_html=email_body_html,
                         scratch_dir=scratch_dir,
                         smtp_server_password=config.smtp_server_password,
                         sending_time=sending_time,
                         logger=logger,
                         sentry_dsn=sentry_dsn)

    store_incident_report_email_info(message, scratch_dir, sending_time=sending_time)

    return message


def send_job_email(
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

    # let's get the needed email template;
    # TODO: should get from pkgresources or so
    env = Environment(loader=FileSystemLoader('%s/../flask_app/templates/' % os.path.dirname(__file__)))
    env.filters['timestamp2isot'] = timestamp2isot
    env.filters['humanize_age'] = humanize_age
    env.filters['humanize_future'] = humanize_future

    # api_code = adapt_line_length_api_code(api_code, line_break="\n", add_line_continuation="\\")
    api_code_no_token = re.sub('"token": ".*?"', '"token": "<PLEASE-INSERT-YOUR-TOKEN-HERE>"', api_code)
    api_code_no_token = wrap_python_code(api_code_no_token)

    api_code = wrap_python_code(api_code)
    api_code_too_long = invalid_email_line_length(api_code) or invalid_email_line_length(api_code_no_token)

    api_code_email_attachment = None
    if api_code_too_long:
        # TODO: send us a sentry alert here
        attachment_file_path = store_email_api_code_attachment(api_code, status, scratch_dir, sending_time=sending_time)
        with open(attachment_file_path, "r") as fil:
            api_code_email_attachment = MIMEApplication(
                fil.read(),
                Name=os.path.basename(attachment_file_path)
            )
        api_code_email_attachment.add_header('Content-Disposition',
                                             'attachment',
                                             filename="api_code.py")

    status_details_message = None
    status_details_title = status
    if status_details is not None and status_details['status'] != 'successful':
        if status_details['status'] == 'empty_product' or status_details['status'] == 'empty_result':
            status_details_message = '''Unfortunately, after a quick automated assessment of the request, it has been found that it contains an <b>empty product</b>.
To the best of our knowledge, no unexpected errors occurred during processing,
and if this is not what you expected, you probably need to modify the request parameters.<br>'''
            status_details_title = 'finished: with empty product'
        # TODO observe the other possible error detected exceptions,and extend the status detail message for the email
        else:
            sentry.capture_message(f'unexpected status_details content before sending email: {status_details}')
            raise NotImplementedError

    # TODO: enable this sometimes
    # compressed_request_url = compress_request_url_params(request_url)

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

    email_data = {
        'oda_site': {
            'site_name': config.site_name,
            'frontend_url': config.products_url,
            'contact': config.contact_email_address,
            'manual_reference': config.email_manual_reference,
        },
        'request': {
            'job_id': job_id,
            'status': status,
            'status_details_title': status_details_title,
            'status_details_message': status_details_message,
            'instrument': instrument,
            'product_type': product_type,
            'time_request': time_request,
            'request_url': possibly_compressed_request_url,
            'api_code_no_token': api_code_no_token,
            'api_code': api_code,
            'api_code_too_long': api_code_too_long,
            'decoded_token': decoded_token,
            'permanent_url': permanent_url,
        }
    }
    template = env.get_template('email.html')
    email_body_html = template.render(**email_data)

    email_subject = re.search("<title>(.*?)</title>", email_body_html).group(1)
    email_text = textify_email(email_body_html)

    if invalid_email_line_length(email_text) or invalid_email_line_length(email_body_html):
        open("debug_email_lines_too_long.html", "w").write(email_body_html)
        open("debug_email_lines_too_long.text", "w").write(email_text)
        raise EMailNotSent(f"email not sent, lines too long!")

    message = send_email(config.smtp_server,
                         config.smtp_port,
                         config.sender_email_address,
                         config.cc_receivers_email_addresses,
                         config.bcc_receivers_email_addresses,
                         tokenHelper.get_token_user_email_address(decoded_token),
                         email_data['oda_site']['contact'],
                         email_subject,
                         email_text,
                         email_body_html,
                         config.smtp_server_password,
                         sending_time=sending_time,
                         scratch_dir=scratch_dir,
                         logger=logger,
                         attachment=api_code_email_attachment,
                         sentry_dsn=sentry_dsn)

    store_status_email_info(message, status, scratch_dir, sending_time=sending_time, first_submitted_time=time_request)

    return message


def send_email(smtp_server,
               smtp_port,
               sender_email_address,
               cc_receivers_email_addresses,
               bcc_receivers_email_addresses,
               receiver_email_addresses,
               reply_to_email_address,
               email_subject,
               email_text,
               email_body_html,
               smtp_server_password,
               logger,
               sending_time=None,
               scratch_dir=None,
               attachment=None,
               sentry_dsn=None
               ):

    server = None
    logger.info(f"Sending email through the smtp server: {smtp_server}:{smtp_port}")
    # Create the plain-text and HTML version of your message,
    # since emails with HTML content might be, sometimes, not supported

    n_tries_left = num_email_sending_max_tries

    if not isinstance(receiver_email_addresses, list):
        receiver_email_addresses = [receiver_email_addresses]
    if cc_receivers_email_addresses is None:
        cc_receivers_email_addresses = []
    if bcc_receivers_email_addresses is None:
        bcc_receivers_email_addresses = []
    # include bcc receivers, which will be hidden in the message header
    receivers_email_addresses = receiver_email_addresses + cc_receivers_email_addresses + bcc_receivers_email_addresses
    # creation of the message
    message = MIMEMultipart("alternative")
    message["Subject"] = email_subject
    message["From"] = sender_email_address
    message["To"] = ", ".join(receiver_email_addresses)
    message["CC"] = ", ".join(cc_receivers_email_addresses)
    message['Reply-To'] = reply_to_email_address

    if attachment is not None:
        # create the attachment
        message.attach(attachment)

    part1 = MIMEText(email_text, "plain")
    part2 = MIMEText(email_body_html, "html")
    message.attach(part1)
    message.attach(part2)

    while True:
        try:
            # Create a secure SSL context
            context = ssl.create_default_context()
            #
            # Try to log in to server and send email
            server = smtplib.SMTP(smtp_server, smtp_port)
            # just for testing purposes, not ssl is established
            if smtp_server != "localhost":
                try:
                    server.starttls(context=context)
                except Exception as e:
                    logger.warning(f'unable to start TLS: {e}')
            if smtp_server_password is not None and smtp_server_password != '':
                server.login(sender_email_address, smtp_server_password)
            server.sendmail(sender_email_address, receivers_email_addresses, message.as_string())
            logger.info("email successfully sent")

            return message
        except Exception as e:
            n_tries_left -= 1

            if n_tries_left > 0:
                logger.warning(f"there seems to be some problem in sending the email with title {email_subject}, "
                               f"another attempt will be made")

                logger.error(f"{e} exception while attempting to send the email with title {email_subject}\n"
                             f"{n_tries_left} tries left, sleeping {email_sending_retry_sleep_s} seconds until retry\n")
                time.sleep(email_sending_retry_sleep_s)
            else:
                logger.warning(f"an issue occurred when sending the email with title {email_subject}, "
                               f"multiple attempts have been executed, but those did not succeed")

                logger.error(f"an issue occurred when sending the email with title {email_subject}, "
                             f"multiple attempts have been executed, the following error has been generated:\n"
                             f"{e}")

                store_not_sent_email(email_body_html, scratch_dir, sending_time=sending_time)

                sentry.capture_message((f'multiple attempts to send an email with title {email_subject} '
                                        f'have been detected, the following error has been generated:\n"'
                                        f'{e}'))

                raise EMailNotSent(f"email not sent: {e}")

        finally:
            if server:
                server.quit()


def store_status_email_info(message, status, scratch_dir, sending_time=None, first_submitted_time=None):
    path_email_history_folder = os.path.join(scratch_dir, 'email_history')
    if not os.path.exists(path_email_history_folder):
        os.makedirs(path_email_history_folder)
    if sending_time is None:
        sending_time = time_.time()
    if first_submitted_time is None:
        first_submitted_time = sending_time

    email_file_name = f'email_{status}_{str(sending_time)}_{str(first_submitted_time)}.email'

    # record the email just sent in a dedicated file
    with open(os.path.join(path_email_history_folder, email_file_name), 'w+') as outfile:
        outfile.write(message.as_string())


def store_not_sent_email(email_body, scratch_dir, sending_time=None):
    path_email_history_folder = os.path.join(scratch_dir, 'email_history')
    if not os.path.exists(path_email_history_folder):
        os.makedirs(path_email_history_folder)
    if sending_time is None:
        sending_time = time_.time()
    # record the email just sent in a dedicated file
    with open(path_email_history_folder + '/not_sent_email_' + str(sending_time) + '.email', 'w+') as outfile:
        outfile.write(email_body)


def store_incident_report_email_info(message, scratch_dir, sending_time=None):
    path_email_history_folder = scratch_dir + '/email_history'
    if not os.path.exists(path_email_history_folder):
        os.makedirs(path_email_history_folder)
    if sending_time is None:
        sending_time = time_.time()
    # record the email just sent in a dedicated file
    with open(path_email_history_folder + '/indident_report_email_' + str(sending_time) +'.email', 'w+') as outfile:
        outfile.write(message.as_string())


def store_email_api_code_attachment(api_code, status, scratch_dir, sending_time=None):
    # email folder
    path_email_history_folder = scratch_dir + '/email_history'
    if not os.path.exists(path_email_history_folder):
        os.makedirs(path_email_history_folder)
    # attachment folder
    path_email_history_attachment = path_email_history_folder + '/attachments'
    if not os.path.exists(path_email_history_attachment):
        os.makedirs(path_email_history_attachment)

    if sending_time is None:
        sending_time = time_.time()
    attachment_file_path = path_email_history_attachment + '/api_code_attachment_' + status + '_' + str(sending_time) + '.py'
    with open(attachment_file_path, 'w+') as outfile:
        outfile.write(api_code)
    return attachment_file_path


def log_email_sending_info(logger, status, time_request, scratch_dir, job_id, additional_info_obj=None):
    path_email_history_folder = os.path.join(scratch_dir, 'email_history')
    if not os.path.exists(path_email_history_folder):
        os.makedirs(path_email_history_folder)
    history_info_obj = dict(time=timestamp2isot(time_request),
                            status=status,
                            job_id=job_id)
    if additional_info_obj is not None:
        history_info_obj['additional_information'] = additional_info_obj
    history_info_obj_hash = make_hash(history_info_obj)
    email_history_log_fn = os.path.join(path_email_history_folder, f'email_history_log_{status}_{time_request}_{history_info_obj_hash}.log')
    with open(email_history_log_fn, 'w') as outfile:
        outfile.write(json.dumps(history_info_obj, indent=4))

    logger.info(f"logging email sending attempt into {email_history_log_fn} file")


def is_email_to_send_run_query(logger, status, time_original_request, scratch_dir, job_id, config, decoded_token=None, sentry_dsn=None):
    log_additional_info_obj = {}
    sending_ok = False
    time_check = time_.time()
    sentry_for_email_sending_check = config.sentry_for_email_sending_check
    # get total request duration
    if decoded_token:
        # in case the job is just submitted and was not submitted before, at least since some time
        logger.info("considering email sending, status: %s, time_original_request: %s", status, time_original_request)

        email_sending_job_submitted = tokenHelper.get_token_user_submitted_email(decoded_token)
        info_parameter = 'extracted from token'
        if email_sending_job_submitted is None:
            # in case this didn't come with the token take the default value from the configuration
            email_sending_job_submitted = config.email_sending_job_submitted
            info_parameter = 'extracted from the configuration'

        log_additional_info_obj['email_sending_job_submitted'] = f'{email_sending_job_submitted}, {info_parameter}'
        logger.info("email_sending_job_submitted: %s", email_sending_job_submitted)

        # get the amount of time passed from when the last email was sent
        interval_ok = True

        email_sending_job_submitted_interval = tokenHelper.get_token_user_sending_submitted_interval_email(decoded_token)
        info_parameter = 'extracted from token'
        if email_sending_job_submitted_interval is None:
            # in case this didn't come with the token take the default value from the configuration
            email_sending_job_submitted_interval = config.email_sending_job_submitted_default_interval
            info_parameter = 'extracted from the configuration'

        logger.info("email_sending_job_submitted_interval: %s", email_sending_job_submitted_interval)
        log_additional_info_obj['email_sending_job_submitted_interval'] = f'{email_sending_job_submitted_interval}, {info_parameter}'

        email_history_dir = os.path.join(scratch_dir + '/email_history')
        logger.info("email_history_dir: %s", email_history_dir)

        email_history_dirs_same_job_id = f"scratch_*_{job_id}*/email_history"
        logger.info("email_history_dirs_same_job_id: %s", email_history_dirs_same_job_id)

        # find all
        submitted_email_pattern = os.path.join(
                email_history_dirs_same_job_id,
                'email_submitted_*.email'
            )
        submitted_email_files = glob.glob(submitted_email_pattern)
        logger.info("submitted_email_files: %s as %s", len(submitted_email_files), submitted_email_pattern)
        log_additional_info_obj['submitted_email_files'] = submitted_email_files

        if len(submitted_email_files) >= 1:
            times = []
            for f in submitted_email_files:
                f_name, f_ext = os.path.splitext(os.path.basename(f))
                if f_ext == '.email' and f_name:
                    times.append(float(f_name.split('_')[2]))

            time_last_email_submitted_sent = max(times)
            time_from_last_submitted_email = time_check - float(time_last_email_submitted_sent)
            interval_ok = time_from_last_submitted_email > email_sending_job_submitted_interval

        logger.info("interval_ok: %s", interval_ok)
        log_additional_info_obj['interval_ok'] = interval_ok

        status_ok = True
        if status != 'submitted':
            status_ok = False
            logger.info(f'status {status} not a valid one for sending an email after a run_query')
            if sentry_for_email_sending_check:
                sentry.capture_message((f'an email sending attempt has been detected at the completion '
                                        f'of the run_query method with the status: {status}'))

        # send submitted mail, status update
        sending_ok = email_sending_job_submitted and interval_ok and status_ok
        if sending_ok:
            log_additional_info_obj['check_result_message'] = 'the email will be sent'
            log_email_sending_info(logger=logger,
                                   status=status,
                                   time_request=time_check,
                                   scratch_dir=scratch_dir,
                                   job_id=job_id,
                                   additional_info_obj=log_additional_info_obj
                                   )
    else:
        logger.info(f'an email will not be sent because a token was not provided')

    return sending_ok


def is_email_to_send_callback(logger, status, time_original_request, scratch_dir, config, job_id, decoded_token=None, sentry_dsn=None):
    log_additional_info_obj = {}
    sending_ok = False
    time_check = time_.time()
    sentry_for_email_sending_check = config.sentry_for_email_sending_check
    if decoded_token:
        # in case the request was long and 'done'
        logger.info("considering email sending, status: %s, time_original_request: %s", status, time_original_request)

        # TODO: could be good to have this configurable
        if status == 'done':
            # get total request duration
            if time_original_request:
                duration_query = time_check - float(time_original_request)
                log_additional_info_obj['query_duration'] = duration_query
            else:
                logger.info(f'time_original_request not available')
                raise MissingRequestParameter('original request time not available')

            timeout_threshold_email = tokenHelper.get_token_user_timeout_threshold_email(decoded_token)
            info_parameter = 'extracted from token'
            if timeout_threshold_email is None:
                # set it to the default value, from the configuration
                timeout_threshold_email = config.email_sending_timeout_default_threshold
                info_parameter = 'extracted from the configuration'

            log_additional_info_obj['timeout_threshold_email'] = f'{timeout_threshold_email}, {info_parameter}'
            logger.info("timeout_threshold_email: %s", timeout_threshold_email)

            email_sending_timeout = tokenHelper.get_token_user_sending_timeout_email(decoded_token)
            info_parameter = 'extracted from token'
            if email_sending_timeout is None:
                email_sending_timeout = config.email_sending_timeout
                info_parameter = 'extracted from the configuration'

            log_additional_info_obj['email_sending_timeout'] = f'{email_sending_timeout}, {info_parameter}'
            logger.info("email_sending_timeout: %s", email_sending_timeout)

            logger.info("duration_query > timeout_threshold_email %s", duration_query > timeout_threshold_email)
            logger.info("email_sending_timeout and duration_query > timeout_threshold_email %s",
                        email_sending_timeout and duration_query > timeout_threshold_email)

            done_email_files = glob.glob(f'scratch_*_jid_{job_id}*/email_history/email_done_*')
            log_additional_info_obj['done_email_files'] = done_email_files
            if len(done_email_files) >= 1:
                logger.info("the email cannot be sent because the number of done emails sent is too high: %s", len(done_email_files))
                raise MultipleDoneEmail("multiple completion email detected")

            sending_ok = tokenHelper.get_token_user_done_email(decoded_token) and email_sending_timeout and \
                         duration_query > timeout_threshold_email

        # or if failed
        elif status == 'failed':
            email_sending_failed = tokenHelper.get_token_user_fail_email(decoded_token)
            log_additional_info_obj['email_sending_failed'] = email_sending_failed
            sending_ok = email_sending_failed

        # not valid status
        else:
            logger.info(f'status {status} not a valid one for sending an email after a callback')
            if sentry_for_email_sending_check:
                sentry.capture_message((f'an email sending attempt has been detected at the completion '
                                        f'of the run_query method with the status: {status}'))
    else:
        logger.info(f'an email will not be sent because a token was not provided')

    if sending_ok:
        log_additional_info_obj['check_result_message'] = 'the email will be sent'
        log_email_sending_info(logger=logger,
                               status=status,
                               time_request=time_check,
                               scratch_dir=scratch_dir,
                               job_id=job_id,
                               additional_info_obj=log_additional_info_obj
                               )

    return sending_ok
