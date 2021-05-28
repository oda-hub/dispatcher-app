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
from jinja2 import Environment, FileSystemLoader

from ..analysis.exceptions import BadRequest, MissingRequestParameter

from datetime import datetime

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

def textify_email(html):
    text = re.search('<body>(.*?)</body>', html, re.S).group(1)

    text = re.sub('<title>.*?</title>', '', text)

    text = re.sub('<a href=(.*?)>(.*?)</a>', r'\2: \1', text)

    return re.sub('<.*?>', '', text)
   

def send_email(
        config,
        logger,
        decoded_token,
        job_id,
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

    api_code_no_token = re.sub('"token": ".*?"', '"token": "<PLEASE-INSERT-YOUR-TOKEN-HERE>"', api_code)\
                        .strip()\
                        .replace("\n", "<br>\n")
    
    email_data = {
        'oda_site': { 
            #TODO: get from config
            'site_name': 'University of Geneva',
            'frontend_url': 'https://www.astro.unige.ch/mmoda', 
            'contact': 'contact@odahub.io'
        },
        'request': {
            'job_id': job_id,
            'status': status,
            'instrument': instrument,
            'product_type': product_type,
            'time_request': time_request,
            'request_url': request_url,
            'api_code_no_token': api_code_no_token,
        }
    }

    template = env.get_template('email.html')
    email_body_html = template.render(**email_data)
    
    email_subject = re.search("<title>(.*?)</title>", email_body_html).groups()[0]    

    email_text = textify_email(email_body_html)
    
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
        receiver_email_address = tokenHelper.get_token_user_email_address(decoded_token)
        receivers_email_addresses = [receiver_email_address] + cc_receivers_email_addresses
        # creation of the message
        message = MIMEMultipart("alternative")
        message["Subject"] = email_subject
        message["From"] = sender_email_address
        message["To"] = receiver_email_address
        message["CC"] = ", ".join(cc_receivers_email_addresses)

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
        raise EMailNotSent(f"email not sent {e}")
    finally:
        if server:
            server.quit()

    store_email_info(message, status, scratch_dir)

    return message


def store_email_info(message, status, scratch_dir):
    path_email_history_folder = scratch_dir + '/email_history'
    if not os.path.exists(path_email_history_folder):
        os.makedirs(path_email_history_folder)
    email_files_list = glob.glob(path_email_history_folder + '/email_*')
    number_emails_scratch_dir = len(email_files_list)
    sending_time = time_.time()
    # record the email just sent in a dedicated file
    with open(path_email_history_folder + '/email_' + str(number_emails_scratch_dir) + '_' + status + '_' + str(sending_time) +'.email', 'w+') as outfile:
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
                'email_*_submitted_*.email'
            )
        submitted_email_files = glob.glob(submitted_email_pattern)
        logger.info("submitted_email_files: %s as %s", len(submitted_email_files), submitted_email_pattern)
        
        if len(submitted_email_files) >= 1:
            last_submitted_email_sent = submitted_email_files[len(submitted_email_files) - 1]
            f_name, f_ext = os.path.splitext(os.path.basename(last_submitted_email_sent))
            time_last_email_submitted_sent = float(f_name.split('_')[3])
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

            done_email_files = glob.glob(f'scratch_*_jid_{job_id}*/email_history')
            if len(done_email_files) >= 1:
                logger.info("number of done emails sent: %s", len(done_email_files))
                raise MultipleDoneEmail("multiple completion email detected")

            return email_sending_timeout and duration_query > timeout_threshold_email

            
        # or if failed
        elif status == 'failed':
            return True

    return False
