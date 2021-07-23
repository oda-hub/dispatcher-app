import time as time_
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import typing
from ..analysis import tokenHelper
import smtplib
import ssl
import os
import re
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

from datetime import datetime

logger = logging.getLogger()

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

    #text = re.search('<body>(.*?)</body>', html, re.S).group(1)

    
    

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


def wrap_python_code(code, max_length=100):

    # this black currently does not split strings without spaces    
    while True:
        new_code = code
        for string_separator in '"', "'":
            new_code = re.sub('(%s[0-9a-zA-Z\.\-\/\+]{%i,}?%s)' % (string_separator, max_length + 1, string_separator), 
                            lambda S: S.group(1)[:max_length] + string_separator + ' ' + string_separator + S.group(1)[max_length:],                         
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


def send_email(
        config,
        logger,
        decoded_token,
        token,
        job_id,
        session_id,
        status="done",
        instrument="",
        product_type="",
        time_request=None,
        request_url="",
        api_code="",
        scratch_dir=None):

    # let's get the needed email template; 
    # TODO: should get from pkgresources or so
    env = Environment(loader=FileSystemLoader('%s/../flask_app/templates/' % os.path.dirname(__file__)))
    env.filters['timestamp2isot'] = timestamp2isot
    env.filters['humanize_age'] = humanize_age
    env.filters['humanize_future'] = humanize_future

    #api_code = adapt_line_length_api_code(api_code, line_break="\n", add_line_continuation="\\")
    api_code = wrap_python_code(api_code)


    #api_code = api_code.strip().replace("\n", "<br>\n")

    api_code_no_token = re.sub('"token": ".*?"', '"token": "<PLEASE-INSERT-YOUR-TOKEN-HERE>"', api_code)

    #  TODO: enable this sometimes
    #   compressed_request_url = compress_request_url_params(request_url)

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
            #TODO: get from config
            'site_name': 'University of Geneva',
            'frontend_url': config.products_url,             
            'contact': 'contact@odahub.io',
            'manual_reference': 'possibly-non-site-specific-link',
        },
        'request': {
            'job_id': job_id,
            'status': status,
            'instrument': instrument,
            'product_type': product_type,
            'time_request': time_request,
            'request_url': possibly_compressed_request_url,
            'api_code_no_token': api_code_no_token,
            'api_code': api_code,
            'decoded_token': decoded_token,
            'permanent_url': permanent_url,
        }
    }

    template = env.get_template('email.html')
    email_body_html = template.render(**email_data)
    
    email_subject = re.search("<title>(.*?)</title>", email_body_html).group(1)
    email_text = textify_email(email_body_html)

    if invalid_email_line_length(email_text) or invalid_email_line_length(email_body_html):
        open("debug_email_not_sent.html", "w").write(email_body_html)
        open("debug_email_not_sent.text", "w").write(email_text)
        raise EMailNotSent(f"email not sent, lines too long!")
    
    server = None
    logger.info("Sending email")
    # Create the plain-text and HTML version of your message,
    # since emails with HTML content might be, sometimes, not supported
        
    try:
        # send the mail with the status update to the mail provided with the token
        # eg done/failed/submitted
        # test with the local server
        smtp_server = config.smtp_server
        port = config.smtp_port
        sender_email_address = config.sender_email_address
        cc_receivers_email_addresses = config.cc_receivers_email_addresses
        bcc_receivers_email_addresses = config.bcc_receivers_email_addresses
        receiver_email_address = tokenHelper.get_token_user_email_address(decoded_token)
        # include bcc receivers, which will be hidden in the message header
        receivers_email_addresses = [receiver_email_address] + cc_receivers_email_addresses + bcc_receivers_email_addresses
        # creation of the message
        message = MIMEMultipart("alternative")
        message["Subject"] = email_subject
        message["From"] = sender_email_address
        message["To"] = receiver_email_address
        message["CC"] = ", ".join(cc_receivers_email_addresses)
        message['Reply-To'] = email_data['oda_site']['contact']

        part1 = MIMEText(email_text, "plain")
        part2 = MIMEText(email_body_html, "html")
        message.attach(part1)
        message.attach(part2)

        smtp_server_password = config.smtp_server_password
        # Create a secure SSL context
        context = ssl.create_default_context()
        #
        # Try to log in to server and send email
        server = smtplib.SMTP(smtp_server, port)
        # just for testing purposes, not ssl is established
        if smtp_server != "localhost":
            try:
                server.starttls(context=context)
            except Exception as e:
                logger.warning(f'unable to start TLS: {e}')
        if smtp_server_password is not None and smtp_server_password != '':
            server.login(sender_email_address, smtp_server_password)
        server.sendmail(sender_email_address, receivers_email_addresses, message.as_string())
    except Exception as e:
        logger.error(f'Exception while sending email: {e}')
        open("debug_email_not_sent.html", "w").write(email_body_html)
        raise EMailNotSent(f"email not sent: {e}")
    finally:
        if server:
            server.quit()

    store_email_info(message, status, scratch_dir)

    return message


def store_email_info(message, status, scratch_dir):
    path_email_history_folder = scratch_dir + '/email_history'
    if not os.path.exists(path_email_history_folder):
        os.makedirs(path_email_history_folder)
    sending_time = time_.time()
    # record the email just sent in a dedicated file
    with open(path_email_history_folder + '/email_' + status + '_' + str(sending_time) +'.email', 'w+') as outfile:
        outfile.write(message.as_string())


def is_email_to_send_run_query(logger, status, time_original_request, scratch_dir, job_id, config, decoded_token=None):
    # get total request duration
    if decoded_token:
        # in case the job is just submitted and was not submitted before, at least since some time
        logger.info("considering email sending, status: %s, time_original_request: %s", status, time_original_request)
        
        email_sending_job_submitted = tokenHelper.get_token_user_submitted_email(decoded_token)
        if email_sending_job_submitted is None:
            # in case this didn't come with the token take the default value from the configuration
            email_sending_job_submitted = config.email_sending_job_submitted

        # get the amount of time passed from when the last email was sent
        interval_ok = True
        
        email_sending_job_submitted_interval = tokenHelper.get_token_user_sending_submitted_interval_email(decoded_token)
        if email_sending_job_submitted_interval is None:
            # in case this didn't come with the token take the default value from the configuration
            email_sending_job_submitted_interval = config.email_sending_job_submitted_default_interval

        logger.info("email_sending_job_submitted_interval: %s", email_sending_job_submitted_interval)

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
        
        if len(submitted_email_files) >= 1:
            times = []
            for f in submitted_email_files:
                f_name, f_ext = os.path.splitext(os.path.basename(f))
                if f_ext == '.email' and f_name:
                    times.append(float(f_name.split('_')[2]))

            time_last_email_submitted_sent = max(times)
            time_from_last_submitted_email = time_.time() - float(time_last_email_submitted_sent)
            interval_ok = time_from_last_submitted_email > email_sending_job_submitted_interval

        logger.info("email_sending_job_submitted: %s", email_sending_job_submitted)
        logger.info("interval_ok: %s", interval_ok)

        # send submitted mail, status update
        return email_sending_job_submitted and interval_ok and status == 'submitted'

    return False


def is_email_to_send_callback(logger, status, time_original_request, config, job_id, decoded_token=None):
    if decoded_token:
        # in case the request was long and 'done'
        logger.info("considering email sending, status: %s, time_original_request: %s", status, time_original_request)

        # TODO: could be good to have this configurable
        if status == 'done':
            # get total request duration
            if time_original_request:
                duration_query = time_.time() - float(time_original_request)
            else:
                raise MissingRequestParameter('original request time not available')
            timeout_threshold_email = tokenHelper.get_token_user_timeout_threshold_email(decoded_token)
            if timeout_threshold_email is None:
                # set it to the a default value, from the configuration
                timeout_threshold_email = config.email_sending_timeout_default_threshold

            logger.info("timeout_threshold_email: %s", timeout_threshold_email)

            email_sending_timeout = tokenHelper.get_token_user_sending_timeout_email(decoded_token)
            if email_sending_timeout is None:
                email_sending_timeout = config.email_sending_timeout

            logger.info("email_sending_timeout: %s", email_sending_timeout)
            logger.info("duration_query > timeout_threshold_email %s", duration_query > timeout_threshold_email)
            logger.info("email_sending_timeout and duration_query > timeout_threshold_email %s", email_sending_timeout and duration_query > timeout_threshold_email)

            done_email_files = glob.glob(f'scratch_*_jid_{job_id}*/email_history/*_done_*')
            if len(done_email_files) >= 1:
                logger.info("number of done emails sent: %s", len(done_email_files))
                raise MultipleDoneEmail("multiple completion email detected")

            return  tokenHelper.get_token_user_done_email(decoded_token) and email_sending_timeout and duration_query > timeout_threshold_email

        # or if failed
        elif status == 'failed':
            return tokenHelper.get_token_user_fail_email(decoded_token)

    return False
