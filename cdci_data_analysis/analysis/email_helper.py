import time as time_
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from ..analysis import tokenHelper
import smtplib
import ssl
import os
import glob

from ..analysis.exceptions import BadRequest, MissingRequestParameter


class EMailNotSent(BadRequest):
    pass


def send_email(
        config,
        logger,
        decoded_token,
        job_id,
        status="done",
        instrument="",
        product_type="",
        time_request=None,
        request_url=""):
    server = None
    logger.info("Sending email")
    # Create the plain-text and HTML version of your message,
    # since emails with HTML content might be, sometimes, not supported
    # a plain-text version is included
    text = f"""Update of the task for the instrument {instrument}:\n* status {status}\nProducts url {request_url}"""
    html = f"""<html><body><p>Update of the task for the instrument {instrument}:<br><ul><li>status {status}</li></ul>Products url {request_url}</p></body></html>"""
    email_subject = f"[ODA][{status}] Request for {product_type} {job_id[:8]}"

    if time_request:
        time_request_str = time_.strftime('%Y-%m-%d %H:%M:%S', time_.localtime(float(time_request)))
        text = f"""Update of the task submitted at {time_request_str}, for the instrument {instrument}:\n* status {status}\nProducts url {request_url}"""
        html = f"""<html><body><p>Update of the task submitted at {time_request_str}, for the instrument {instrument}:<br><ul><li>status {status}</li></ul>Products url {request_url}</p></body></html>"""
        email_subject = f"[ODA][{status}] Request for {product_type} created at {time_request_str} {job_id[:8]}"

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

        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html")
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


def is_email_to_send_run_completion(logger, status, time_original_request, scratch_dir, config, decoded_token=None):
    # get total request duration
    if decoded_token:
        # in case the request was long and 'done'
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

        if os.path.exists(scratch_dir + '/email_history'):
            submitted_email_files = glob.glob(scratch_dir + '/email_history/email_*_submitted_*.email')
            logger.info("submitted_email_files: %s", len(submitted_email_files))
            if len(submitted_email_files) >= 1:
                last_submitted_email_sent = submitted_email_files[len(submitted_email_files) - 1]
                f_name, f_ext = os.path.splitext(os.path.basename(last_submitted_email_sent))
                time_last_email_submitted_sent = float(f_name.split('_')[3])
                time_from_last_submitted_email = time_.time() - float(time_last_email_submitted_sent)
                interval_ok = time_from_last_submitted_email > email_sending_job_submitted_interval

        # send submitted mail, status update
        return email_sending_job_submitted and interval_ok and status == 'submitted'

    return False


def is_email_to_send_callback(logger, status, time_original_request, config, decoded_token=None):
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

            return email_sending_timeout and duration_query > timeout_threshold_email

        # or if failed
        elif status == 'failed':
            return True

    return False
