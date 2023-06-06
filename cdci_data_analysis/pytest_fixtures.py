# this could be a separate package or/and a pytest plugin
from json import JSONDecodeError

import sentry_sdk
import yaml

import cdci_data_analysis.flask_app.app
from cdci_data_analysis.analysis.exceptions import BadRequest
from cdci_data_analysis.flask_app.dispatcher_query import InstrumentQueryBackEnd
from cdci_data_analysis.analysis.hash import make_hash
from cdci_data_analysis.configurer import ConfigEnv

import re
import json
import string
import random
import requests
import logging
import shutil
import tempfile
import pytest
import subprocess
import os
import signal
import psutil
import copy
import time
import hashlib
import glob

from git import Repo
from threading import Thread

__this_dir__ = os.path.join(os.path.abspath(os.path.dirname(__file__)))

logger = logging.getLogger()

def kill_child_processes(parent_pid, sig=signal.SIGINT):
    try:
        parent = psutil.Process(parent_pid)
        children = parent.children(recursive=True)
        for process in children:
            process.send_signal(sig)
    except psutil.NoSuchProcess:
        return


@pytest.fixture(scope="session")
def app():
    app = cdci_data_analysis.flask_app.app.app
    return app


@pytest.fixture
def sentry_sdk_fixture(monkeypatch, dispatcher_test_conf):
    sentry_sdk.init(
        dsn=dispatcher_test_conf['sentry_url'],
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0,
        debug=True,
        max_breadcrumbs=50,
    )


@pytest.fixture
def dispatcher_debug(monkeypatch):
    monkeypatch.setenv('DISPATCHER_DEBUG_MODE', 'yes')


@pytest.fixture
def gunicorn_dispatcher(monkeypatch):
    monkeypatch.setenv('GUNICORN_DISPATCHER', 'yes')


@pytest.fixture
def gunicorn_tmp_path(monkeypatch):
    monkeypatch.setenv('GUNICORN_TMP_PATH', '/tmp/dispatcher-test-fixture-state-gunicorn-{}.json')


@pytest.fixture
def default_params_dict():
    params = dict(
        query_status="new",
        query_type="Real",
        instrument="isgri",
        product_type="isgri_image",
        osa_version="OSA10.2",
        E1_keV=20.,
        E2_keV=40.,
        T1="2008-01-01T11:11:11.0",
        T2="2009-01-01T11:11:11.0",
        max_pointings=2,
        RA=83,
        DEC=22,
        radius=6,
        async_dispatcher=False
    )
    yield params


@pytest.fixture
def default_token_payload():
    default_exp_time = int(time.time()) + 5000
    default_token_payload = dict(
        sub="mtm@mtmco.net",
        name="mmeharga",
        roles="general",
        exp=default_exp_time,
        tem=0,
        mstout=True,
        mssub=True,
        intsub=5
    )

    yield default_token_payload


@pytest.fixture
def dispatcher_nodebug(monkeypatch):
    monkeypatch.delenv('DISPATCHER_DEBUG_MODE', raising=False)


def run_analysis(server, params, method='get', files=None):
    if method == 'get':
        if files is not None:
            logger.error("files cannot be attached to a get request")
            raise BadRequest("Invalid parameters for GET request")
        return requests.get(os.path.join(server, "run_analysis"),
                    params={**params},
                    )

    elif method == 'post':
        return requests.post(os.path.join(server, "run_analysis"),
                    data={**params},
                    files=files
                    )
    else:
        raise NotImplementedError


def ask(server, params, expected_query_status, expected_job_status=None, max_time_s=None, expected_status_code=200, method='get', files=None):
    t0 = time.time()

    c = run_analysis(server, params, method=method, files=files)

    logger.info(f"\033[31m request took {time.time() - t0} seconds\033[0m")
    t_spent = time.time() - t0

    if max_time_s is not None:
        assert t_spent < max_time_s

    logger.info("content: %s", c.text[:2000])
    if len(c.text) > 2000:
        print(".... (truncated)")

    jdata = c.json()

    if expected_status_code is not None:
        assert c.status_code == expected_status_code

    logger.info(list(jdata.keys()))

    if expected_job_status is not None:
        assert jdata["exit_status"]["job_status"] in expected_job_status

    if expected_query_status is not None:
        assert jdata["query_status"] in expected_query_status

    return jdata


def loop_ask(server, params, method='get', max_time_s=None, async_dispatcher=False):
    jdata = ask(server,
                {**params, 
                'async_dispatcher': async_dispatcher,
                'query_status': 'new',
                },
                expected_query_status=["submitted", "done"],
                method=method,
                )

    last_status = jdata["query_status"]

    t0 = time.time()

    tries_till_reset = 20

    while True:
        if tries_till_reset <= 0:
            next_query_status = "ready"
            print("\033[1;31;46mresetting query status to new, too long!\033[0m")
            tries_till_reset = 20
        else:
            next_query_status = jdata['query_status']
            tries_till_reset -= 1

        jdata = ask(server,
                    {**params, "async_dispatcher": async_dispatcher,
                            'query_status': next_query_status,
                            'job_id': jdata['job_monitor']['job_id'],
                            'session_id': jdata['session_id']},
                    expected_query_status=["submitted", "done"],
                    max_time_s=max_time_s,
                    )

        if jdata["query_status"] in ["ready", "done"]:
            logger.info("query READY: %s", jdata["query_status"])
            break

        logger.info("query NOT-READY: %s monitor %s", jdata["query_status"], jdata["job_monitor"])
        logger.info("looping...")

        time.sleep(5)

    logger.info(f"\033[31m total request took {time.time() - t0} seconds\033[0m")


    return jdata, time.time() - t0

def validate_no_data_products(jdata):
    assert jdata["exit_status"]["debug_message"] == "{\"node\": \"dataanalysis.core.AnalysisException\", \"exception\": \"{}\", \"exception_kind\": \"handled\"}"
    assert jdata["exit_status"]["error_message"] == "AnalysisException:{}"
    assert jdata["exit_status"]["message"] == "failed: get dataserver products "
    assert jdata["job_status"] == "failed"




@pytest.fixture
def dispatcher_local_mail_server(pytestconfig, dispatcher_test_conf):
    from aiosmtpd.controller import Controller

    class CustomController(Controller):
        def __init__(self, id, handler, hostname='127.0.0.1', port=dispatcher_test_conf['email_options']['smtp_port']):
            self.id = id
            super().__init__(handler, hostname=hostname, port=port)

        @property
        def local_smtp_output_json_fn(self):
            return self.handler.output_file_path

        @property
        def local_smtp_output(self):
            return json.load(open(self.local_smtp_output_json_fn))

        def assert_email_number(self, N):
            f_local_smtp_jdata = self.local_smtp_output
            assert len(f_local_smtp_jdata) == N, f"found {len(f_local_smtp_jdata)} emails, expected == {N}"

        def get_email_record(self, i=0, N=None):
            if N is not None:
                assert i < N
                self.assert_email_number(N)

            return self.local_smtp_output[i]
            
            



    class CustomHandler:
        def __init__(self, output_file_path):
            self.output_file_path = output_file_path

        async def handle_DATA(self, server, session, envelope):
            try:
                obj_email_data = dict(
                    mail_from=envelope.mail_from,
                    rcpt_tos=envelope.rcpt_tos,
                    data=envelope.content.decode()
                )
                peer = session.peer
                mail_from = envelope.mail_from
                rcpt_tos = envelope.rcpt_tos
                data = envelope.content
                print(f"mail server: Receiving message from: {peer}")
                print(f"mail server: Message addressed from: {mail_from}")
                print(f"mail server: Message addressed to: {rcpt_tos}")
                print(f"mail server: Message length : {len(data)}")

                # log in a file
                l = []
                if os.path.exists(self.output_file_path):
                    with open(self.output_file_path, 'r') as readfile:
                        try:
                            l = json.load(readfile)
                        except JSONDecodeError as e:
                            pass
                with open(self.output_file_path, 'w+') as outfile:
                    l.append(obj_email_data)
                    json.dump(l, outfile)

            except Exception as e:
                return '500 Could not process your message'
            return '250 OK'

    id = u''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))
    if not os.path.exists('local_smtp_log'):
        os.makedirs('local_smtp_log')

    fn =f'local_smtp_log/{id}_local_smtp_output.json'
    handler = CustomHandler(fn)
    controller = CustomController(id, handler, hostname='127.0.0.1', port=dispatcher_test_conf['email_options']['smtp_port'])
    # Run the event loop in a separate thread
    controller.start()

    yield controller

    print("will stop the mail server")
    controller.stop()


@pytest.fixture
def dispatcher_local_mail_server_subprocess(pytestconfig, dispatcher_test_conf):
    import subprocess
    import os
    import copy
    from threading import Thread

    env = copy.deepcopy(dict(os.environ))
    print(("rootdir", str(pytestconfig.rootdir)))
    env['PYTHONPATH'] = str(pytestconfig.rootdir) + ":" + str(pytestconfig.rootdir) + "/tests:" + env.get('PYTHONPATH',
                                                                                                          "")
    print(("pythonpath", env['PYTHONPATH']))

    cmd = [
        "python",
        "-m", "smtpd",
        "-c", "DebuggingServer",
        "-n", 
        f"localhost:{dispatcher_test_conf['email_options']['smtp_port']}"
    ]

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        env=env,
    )

    def follow_output():
        for line in iter(p.stdout):
            line = line.decode()
            print(f"mail server: {line.rstrip()}")

    thread = Thread(target=follow_output, args=())
    thread.start()

    yield thread

    print("will stop local mail server")
    print(("child:", p.pid))
    import os, signal
    kill_child_processes(p.pid, signal.SIGINT)
    os.kill(p.pid, signal.SIGINT)


@pytest.fixture
def dispatcher_test_conf_fn(tmpdir):
    fn = os.path.join(tmpdir, "test-dispatcher-conf.yaml")
    with open(fn, "w") as f:
        f.write("""
dispatcher:
    dummy_cache: dummy-cache
    products_url: PRODUCTS_URL
    dispatcher_callback_url_base: http://0.0.0.0:8011
    sentry_url: "https://2ba7e5918358439485632251fa73658c@sentry.io/1467382"
    logstash_host: 
    logstash_port: 
    secret_key: 'secretkey_test'
    token_max_refresh_interval: 604800
    soft_minimum_folder_age_days: 5
    hard_minimum_folder_age_days: 30
    bind_options:
        bind_host: 0.0.0.0
        bind_port: 8011
    email_options:
        smtp_server: 'localhost'
        site_name: 'University of Geneva'
        manual_reference: 'possibly-non-site-specific-link'
        sender_email_address: 'team@odahub.io'
        contact_email_address: 'contact@odahub.io'
        cc_receivers_email_addresses: ['team@odahub.io']
        bcc_receivers_email_addresses: ['teamBcc@odahub.io']
        smtp_port: 61025
        smtp_server_password: ''
        email_sending_timeout: True
        email_sending_timeout_default_threshold: 1800
        email_sending_job_submitted: True
        email_sending_job_submitted_default_interval: 60
        sentry_for_email_sending_check: False
        incident_report_email_options:
            incident_report_sender_email_address: 'postmaster@in.odahub.io'
            incident_report_receivers_email_addresses: ['team@odahub.io']
    """)

    yield fn


@pytest.fixture
def dispatcher_test_conf_empty_sentry_fn(dispatcher_test_conf_fn):
    fn = dispatcher_test_conf_fn
    with open(fn, "r+") as f:
        data = f.read()
        data = re.sub('(\s+sentry_url:).*\n', r'\1\n', data)
        f.seek(0)
        f.write(data)
        f.truncate()

    yield fn


@pytest.fixture
def dispatcher_test_conf_no_products_url_fn(dispatcher_test_conf_fn):
    fn = dispatcher_test_conf_fn
    with open(fn, "r+") as f:
        data = f.read()
        data = re.sub('(\s+products_url:).*\n', r'\1\n', data)
        f.seek(0)
        f.write(data)
        f.truncate()

    yield fn


@pytest.fixture
def dispatcher_test_conf_with_external_products_url_fn(dispatcher_test_conf_fn):
    fn = dispatcher_test_conf_fn
    with open(fn, "r+") as f:
        data = f.read()
        data = re.sub('(\s+products_url:).*\n', '\n    products_url: http://localhost:1234/mmoda/\n', data)
        f.seek(0)
        f.write(data)
        f.truncate()

    yield fn


@pytest.fixture
def dispatcher_test_conf_with_gallery_fn(dispatcher_test_conf_fn):
    fn = "test-dispatcher-conf-with-gallery.yaml"

    with open(fn, "w") as f:
        with open(dispatcher_test_conf_fn) as f_default:
            f.write(f_default.read())

        f.write('\n    product_gallery_options:'
                '\n        product_gallery_url: "http://cdciweb02.isdc.unige.ch/mmoda/galleryd"'
                f'\n        product_gallery_secret_key: "{os.getenv("DISPATCHER_PRODUCT_GALLERY_SECRET_KEY", "secret_key")}"'
                '\n        product_gallery_timezone: "Europe/Zurich"'
                '\n        name_resolver_url: "https://resolver-prod.obsuks1.unige.ch/api/v1.1/byname/{}"'
                '\n        entities_portal_url: "http://cdsportal.u-strasbg.fr/?target={}"'
                '\n        converttime_revnum_service_url: "https://www.astro.unige.ch/mmoda/dispatch-data/gw/timesystem/api/v1.0/converttime/UTC/{}/REVNUM"')

    yield fn


@pytest.fixture
def dispatcher_test_conf_with_gallery_no_resolver_fn(dispatcher_test_conf_fn):
    fn = "test-dispatcher-conf-with-gallery.yaml"

    with open(fn, "w") as f:
        with open(dispatcher_test_conf_fn) as f_default:
            f.write(f_default.read())

        f.write('\n    product_gallery_options:'
                '\n        product_gallery_url: "http://cdciweb02.isdc.unige.ch/mmoda/galleryd"'
                '\n        product_gallery_timezone: "Europe/Zurich"'
                f'\n        product_gallery_secret_key: "{os.getenv("DISPATCHER_PRODUCT_GALLERY_SECRET_KEY", "secret_key")}"')

    yield fn


@pytest.fixture
def dispatcher_test_conf_with_renku_options_fn(dispatcher_test_conf_fn):
    fn = "test-dispatcher-conf-with-renku-options.yaml"
    filesys_repo = 'file:///renkulab.io/gitlab/gabriele.barni/test-dispatcher-endpoint'
    with open(fn, "w") as f:
        with open(dispatcher_test_conf_fn) as f_default:
            f.write(f_default.read())

        f.write('\n    renku_options:'
                '\n        renku_gitlab_repository_url: "git@renkulab.io:gabriele.barni/old-test-dispatcher-endpoint.git"'
                '\n        renku_base_project_url: "http://renkulab.io/projects"'
               f'\n        ssh_key_path: "{os.getenv("SSH_KEY_FILE", "ssh_key_file")}"')

    yield fn


@pytest.fixture
def dispatcher_test_conf_no_products_url(dispatcher_test_conf_no_products_url_fn):
    with open(dispatcher_test_conf_no_products_url_fn) as yaml_f:
        loaded_yaml = yaml.load(yaml_f, Loader=yaml.SafeLoader)
    yield loaded_yaml['dispatcher']


@pytest.fixture
def dispatcher_test_conf_with_external_products_url(dispatcher_test_conf_with_external_products_url_fn):
    with open(dispatcher_test_conf_with_external_products_url_fn) as yaml_f:
        loaded_yaml = yaml.load(yaml_f, Loader=yaml.SafeLoader)
    yield loaded_yaml['dispatcher']


@pytest.fixture
def dispatcher_test_conf_with_gallery(dispatcher_test_conf_with_gallery_fn):
    yield yaml.load(open(dispatcher_test_conf_with_gallery_fn), Loader=yaml.SafeLoader)['dispatcher']


@pytest.fixture
def dispatcher_test_conf_with_gallery_no_resolver(dispatcher_test_conf_with_gallery_no_resolver_fn):
    yield yaml.load(open(dispatcher_test_conf_with_gallery_no_resolver_fn), Loader=yaml.SafeLoader)['dispatcher']


@pytest.fixture
def dispatcher_test_conf_with_renku_options(dispatcher_test_conf_with_renku_options_fn):
    yield yaml.load(open(dispatcher_test_conf_with_renku_options_fn), Loader=yaml.SafeLoader)['dispatcher']


@pytest.fixture
def dispatcher_test_conf(dispatcher_test_conf_fn):
    yield yaml.load(open(dispatcher_test_conf_fn), Loader=yaml.SafeLoader)['dispatcher']


def start_dispatcher(rootdir, test_conf_fn, multithread=False, gunicorn=False):
    clean_test_dispatchers()

    env = copy.deepcopy(dict(os.environ))
    print(("rootdir", str(rootdir)))
    env['PYTHONPATH'] = str(rootdir) + ":" + str(rootdir) + "/tests:" + \
                        str(rootdir) + '/bin:' + \
                        __this_dir__ + ":" + os.path.join(__this_dir__, "../bin:") + \
                        env.get('PYTHONPATH', "")
    print(("pythonpath", env['PYTHONPATH']))

    if gunicorn:

        conf = ConfigEnv.from_conf_file(test_conf_fn,
                                        set_by=f'command line {__file__}:{__name__}')

        dispatcher_bind_host = conf.bind_host
        dispatcher_bind_port = conf.bind_port
        cmd = [
            "gunicorn",
            f"cdci_data_analysis.flask_app.app:conf_app(\"{test_conf_fn}\")",
            "--bind", f"{dispatcher_bind_host}:{dispatcher_bind_port}",
            "--workers", "8",
            "--threads", "2",
            "--preload",
            "--timeout", "900",
            "--limit-request-line", "0",
            "--log-level", "debug"
        ]

    else:
        fn = os.path.join(__this_dir__, "../bin/run_osa_cdci_server.py")
        if os.path.exists(fn):
            cmd = [
                     "python",
                     fn
                  ]
        else:
            cmd = [
                     "run_osa_cdci_server.py"
                  ]

        cmd += [
                "-d",
                "-conf_file", test_conf_fn,
                "-debug",
              ]

        if multithread:
            cmd += ['-multithread']

    print(f"\033[33mcommand: {cmd}\033[0m")

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        env=env,
    )

    url_store=[None]
    def follow_output():
        url_store[0] = None
        for line in iter(p.stdout):
            line = line.decode()

            NC = '\033[0m'
            if 'ERROR' in line:
                C = '\033[31m'
            else:
                C = '\033[34m'

            print(f"{C}following server: {line.rstrip()}{NC}" )
            if gunicorn:
                m = re.search(r"Listening at: (.*?) (.*?)\n", line)
                if m:
                    url_store[0] = m.group(1).strip()  # alternatively get from configenv
                    print(f"{C}following server: found url:{url_store[0]}")
            else:
                m = re.search(r"Running on (.*?) \(Press CTRL\+C to quit\)", line)
                if m:
                    url_store[0] = m.group(1).strip()  # alternatively get from configenv
                    print(f"{C}following server: found url:{url_store[0]}")

                if re.search("\* Debugger PIN:.*?", line):
                    url_store[0] = url_store[0].replace("0.0.0.0", "127.0.0.1")
                    print(f"{C}following server: server ready, url {url_store[0]}")


    thread = Thread(target=follow_output, args=())
    thread.start()

    started_waiting = time.time()
    while url_store[0] is None:
        print("waiting for server to start since", time.time() - started_waiting)
        time.sleep(0.2)
    time.sleep(0.5)

    service = url_store[0]

    return dict(
        url=service,
        pid=p.pid
    )


@pytest.fixture
def gunicorn_dispatcher_long_living_fixture(gunicorn_tmp_path, gunicorn_dispatcher, dispatcher_long_living_fixture):
    yield dispatcher_long_living_fixture


@pytest.fixture
def dispatcher_long_living_fixture(pytestconfig, dispatcher_test_conf_fn, dispatcher_debug):
    tmp_path = "/tmp/dispatcher-test-fixture-state-{}.json"
    if os.environ.get('GUNICORN_TMP_PATH', None) is not None:
        tmp_path = os.environ.get('GUNICORN_TMP_PATH')

    dispatcher_state_fn = tmp_path.format(
        hashlib.md5(open(dispatcher_test_conf_fn, "rb").read()).hexdigest()[:8]
        )

    if os.path.exists(dispatcher_state_fn):
        dispatcher_state = json.load(open(dispatcher_state_fn))
        logger.info("\033[31mfound dispatcher state: %s\033[0m", dispatcher_state)

        status_code = None

        try:
            r = requests.get(dispatcher_state['url'] + "/run_analysis")
            logger.info("dispatcher returns: %s, %s", r.status_code, r.text)
            logger.info("dispatcher response: %s %s", r.status_code, r.text)
            if r.status_code in [200, 400]:
                logger.info("dispatcher is live and responsive")
                return dispatcher_state['url']                
            status_code = r.status_code
        except requests.exceptions.ConnectionError as e:
            logger.warning("\033[31mdispatcher connection failed %s\033[0m", e)        
        
        logger.warning("\033[31mdispatcher is dead or unresponsive: %s\033[0m", status_code)
    else:
        logger.info("\033[31mdoes not exist!\033[0m")

    gunicorn = False
    if os.environ.get('GUNICORN_DISPATCHER', 'no') == 'yes':
        gunicorn = True

    dispatcher_state = start_dispatcher(pytestconfig.rootdir, dispatcher_test_conf_fn, gunicorn=gunicorn)
    json.dump(dispatcher_state, open(dispatcher_state_fn, "w"))
    return dispatcher_state['url']


@pytest.fixture
def empty_products_files_fixture(default_params_dict):
    #TODO: avoid copypaste in empty_products_user_files_fixture
    # generate job_id
    job_id = make_hash(InstrumentQueryBackEnd.restricted_par_dic(default_params_dict))
    # generate random session_id
    session_id = DispatcherJobState.generate_session_id()
    scratch_params = dict(
        job_id=job_id,
        session_id= session_id
    )
    DispatcherJobState.remove_scratch_folders(job_id=job_id)
    DispatcherJobState.remove_download_folders()
    scratch_dir_path = f'scratch_sid_{session_id}_jid_{job_id}'
    # set the scratch directory
    os.makedirs(scratch_dir_path)

    with open(scratch_dir_path + '/test.fits.gz', 'wb') as fout:
        scratch_params['content'] = os.urandom(20)
        fout.write(scratch_params['content'])

    with open(scratch_dir_path + '/analysis_parameters.json', 'w') as outfile:
        my_json_str = json.dumps(default_params_dict, indent=4)
        outfile.write(u'%s' % my_json_str)

    yield scratch_params


@pytest.fixture
def empty_products_user_files_fixture(default_params_dict, default_token_payload):
    sub = default_token_payload['sub']
    
    # generate job_id related to a certain user    
    job_id = make_hash(
            {
                **InstrumentQueryBackEnd.restricted_par_dic(default_params_dict),
                 "sub": sub
            }
        )

    # generate random session_id
    session_id = u''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))
    scratch_params = dict(
        job_id=job_id,
        session_id= session_id
    )
    DispatcherJobState.remove_scratch_folders(job_id=job_id)
    DispatcherJobState.remove_download_folders()
    
    scratch_dir_path = f'scratch_sid_{session_id}_jid_{job_id}'
    # set the scratch directory
    os.makedirs(scratch_dir_path)

    with open(scratch_dir_path + '/test.fits.gz', 'wb') as fout:
        scratch_params['content'] = os.urandom(20)
        fout.write(scratch_params['content'])

    with open(scratch_dir_path + '/analysis_parameters.json', 'w') as outfile:
        my_json_str = json.dumps(default_params_dict, indent=4)
        outfile.write(u'%s' % my_json_str)

    yield scratch_params


@pytest.fixture
def dispatcher_live_fixture(pytestconfig, dispatcher_test_conf_fn, dispatcher_debug, request):
    if os.getenv('TEST_ONLY_FAST') == 'true':
        # in this case, run all dispatchers long-living, since it's faster but less safe
        yield request.getfixturevalue('dispatcher_long_living_fixture')
    else:
        gunicorn = False
        if os.environ.get('GUNICORN_DISPATCHER', 'no') == 'yes':
            gunicorn = True
        # TODO has to be improved
        if hasattr(request, 'param') and request.param is not None and isinstance(request.param, tuple):
            param_name = request.param[0]
            param_value = request.param[1]
            if param_value is not None:
                fn = f"test-dispatcher-conf-with-{param_name}-param.yaml"

                with open(dispatcher_test_conf_fn) as f_default:
                    disp_conf = yaml.safe_load(f_default.read())
                disp_conf['dispatcher'][param_name] = param_value

                with open(fn, "w") as f:
                    yaml.dump(disp_conf, f)

                dispatcher_test_conf_fn = fn

        dispatcher_state = start_dispatcher(pytestconfig.rootdir, dispatcher_test_conf_fn, gunicorn=gunicorn)

        service = dispatcher_state['url']
        pid = dispatcher_state['pid']

        yield service
                
        kill_child_processes(pid, signal.SIGINT)
        os.kill(pid, signal.SIGINT)


@pytest.fixture
def gunicorn_dispatcher_live_fixture(gunicorn_dispatcher, dispatcher_live_fixture):
    yield dispatcher_live_fixture

@pytest.fixture
def dispatcher_live_fixture_empty_sentry(pytestconfig, dispatcher_test_conf_empty_sentry_fn, dispatcher_debug):
    dispatcher_state = start_dispatcher(pytestconfig.rootdir, dispatcher_test_conf_empty_sentry_fn)

    service = dispatcher_state['url']
    pid = dispatcher_state['pid']

    yield service

    print(("child:", pid))
    kill_child_processes(pid, signal.SIGINT)
    os.kill(pid, signal.SIGINT)


@pytest.fixture
def dispatcher_live_fixture_with_gallery(pytestconfig, dispatcher_test_conf_with_gallery_fn, dispatcher_debug):
    dispatcher_state = start_dispatcher(pytestconfig.rootdir, dispatcher_test_conf_with_gallery_fn)

    service = dispatcher_state['url']
    pid = dispatcher_state['pid']

    yield service

    kill_child_processes(pid, signal.SIGINT)
    os.kill(pid, signal.SIGINT)


@pytest.fixture
def dispatcher_live_fixture_with_gallery_no_resolver(pytestconfig, dispatcher_test_conf_with_gallery_no_resolver_fn, dispatcher_debug):
    dispatcher_state = start_dispatcher(pytestconfig.rootdir, dispatcher_test_conf_with_gallery_no_resolver_fn)

    service = dispatcher_state['url']
    pid = dispatcher_state['pid']

    yield service

    kill_child_processes(pid, signal.SIGINT)
    os.kill(pid, signal.SIGINT)


@pytest.fixture
def dispatcher_live_fixture_no_products_url(pytestconfig, dispatcher_test_conf_no_products_url_fn, dispatcher_debug):
    dispatcher_state = start_dispatcher(pytestconfig.rootdir, dispatcher_test_conf_no_products_url_fn)

    service = dispatcher_state['url']
    pid = dispatcher_state['pid']

    yield service

    kill_child_processes(pid, signal.SIGINT)
    os.kill(pid, signal.SIGINT)


@pytest.fixture
def dispatcher_live_fixture_with_external_products_url(pytestconfig, dispatcher_test_conf_with_external_products_url_fn, dispatcher_debug):
    dispatcher_state = start_dispatcher(pytestconfig.rootdir, dispatcher_test_conf_with_external_products_url_fn)

    service = dispatcher_state['url']
    pid = dispatcher_state['pid']

    yield service

    kill_child_processes(pid, signal.SIGINT)
    os.kill(pid, signal.SIGINT)


@pytest.fixture
def dispatcher_live_fixture_with_renku_options(pytestconfig, dispatcher_test_conf_with_renku_options_fn, dispatcher_debug):
    dispatcher_state = start_dispatcher(pytestconfig.rootdir, dispatcher_test_conf_with_renku_options_fn)

    service = dispatcher_state['url']
    pid = dispatcher_state['pid']

    yield service

    kill_child_processes(pid, signal.SIGINT)
    os.kill(pid, signal.SIGINT)


@pytest.fixture
def dispatcher_live_fixture_no_debug_mode(pytestconfig, dispatcher_test_conf_fn, dispatcher_nodebug):
    dispatcher_state = start_dispatcher(pytestconfig.rootdir, dispatcher_test_conf_fn)

    service = dispatcher_state['url']
    pid = dispatcher_state['pid']

    yield service

    print(("child:", pid))

    kill_child_processes(pid, signal.SIGINT)
    os.kill(pid, signal.SIGINT)


dispatcher_dummy_product_pack_state_fn = "/tmp/dispatcher-dummy-product-pack-ready"


def clean_test_dispatchers():
    for fn in glob.glob("/tmp/dispatcher-test-fixture-state*json"):
        dispatcher_state = json.load(open(fn))
        pid = dispatcher_state['pid']

        try:
            print("child:", pid)
            kill_child_processes(pid,signal.SIGINT)
            os.kill(pid, signal.SIGINT)
        except Exception as e:
            print("unable to cleanup dispatcher", dispatcher_state)

        os.remove(fn)

    if os.path.exists(dispatcher_dummy_product_pack_state_fn):
        os.remove(dispatcher_dummy_product_pack_state_fn)


@pytest.fixture(scope="session", autouse=True)
def cleanup(request):    
    request.addfinalizer(clean_test_dispatchers)
    


def dispatcher_fetch_dummy_products(dummy_product_pack: str, reuse=False):
    url_base = "https://www.isdc.unige.ch/~savchenk" # TODO: to move somewhere to github
    url = f"{url_base}/dispatcher-plugin-integral-data-dummy_prods-{dummy_product_pack}.tgz"

    if reuse:
        if os.path.exists(dispatcher_dummy_product_pack_state_fn):
            logging.info("dispatcher_dummy_product_pack_state_fn: %s found, returning", dispatcher_dummy_product_pack_state_fn)
            return
    
    temp_handle, temp_file_name = tempfile.mkstemp(suffix=f"dummy_product_pack-{dummy_product_pack}")    
    
    with os.fdopen(temp_handle, "wb") as f:        
        logging.info("\033[32mdownloading %s\033[0m", url)
        response = requests.get(url)

        if response.status_code != 200:
            raise RuntimeError(f"can not file dummy_pack {dummy_product_pack} at {url}")

        logging.info("\033[32mfound content length %s\033[0m", len(response.content))
        
        #map(f.write, response.iter_content(1024))

        f.write(response.content)

    dummy_base_dir = os.getcwd()
    shutil.unpack_archive(temp_file_name, extract_dir=dummy_base_dir, format="gztar")
    logging.info("\033[32munpacked to %s\033[0m", dummy_base_dir)

    os.remove(temp_file_name)

    open(dispatcher_dummy_product_pack_state_fn, "w").write("%s"%time.time())


class DispatcherJobState:
    """
    manages state stored in scratch_* directories
    """

    @staticmethod
    def extract_api_code(session_id, job_id):
        # check query output are generated
        query_output_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/query_output.json'
        # the aliased version might have been created
        query_output_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/query_output.json'
        # get the query output
        if os.path.exists(query_output_json_fn):
            f = open(query_output_json_fn)
        else:
            f = open(query_output_json_fn_aliased)

        query_output_data = json.load(f)
        extracted_api_code = None
        if 'prod_dictionary' in query_output_data and 'api_code' in query_output_data['prod_dictionary']:
            extracted_api_code = query_output_data['prod_dictionary']['api_code']

        return extracted_api_code

    @staticmethod
    def generate_session_id():
        return u''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))

    @staticmethod
    def create_temp_folder(session_id, job_id=None):
        suffix = ""

        if session_id is not None:
            suffix += '_sid_' + session_id

        if job_id is not None:
            suffix += '_jid_' + job_id

        td = tempfile.mkdtemp(suffix=suffix)
        return td

    @staticmethod
    def remove_scratch_folders(job_id=None):
        if job_id is None:
            dir_list = glob.glob('scratch_*')
        else:
            dir_list = glob.glob(f'scratch_*_jid_{job_id}*')
        for d in dir_list:
            shutil.rmtree(d)

    @staticmethod
    def remove_download_folders(id=None):
        if id is None:
            dir_list = glob.glob('download_*')
        else:
            dir_list = glob.glob(f'download_{id}')
        for d in dir_list:
            shutil.rmtree(d)

    @staticmethod
    def create_p_value_file(p_value):
        # generate ScWs list file
        if not os.path.exists('p_value_simple_files'):
            os.makedirs('p_value_simple_files')

        # hash file content
        p_hash = make_hash(p_value)

        file_name = f'p_{p_hash}'

        with open('p_value_simple_files/' + file_name, 'w+') as outlist_file:
            outlist_file.write(str(p_value))
        return f'p_value_simple_files/{file_name}'

    @staticmethod
    def create_scw_list_file(list_length, format='list', scw_list=None):
        # generate ScWs list file
        if not os.path.exists('scw_list_files'):
            os.makedirs('scw_list_files')

        # scw_list

        if scw_list is None:
            # this takes priority; allows to avoid repetition
            scw_list = [f"0665{i:04d}0010.001" for i in range(list_length)]

        # hash file content
        scw_list_hash = make_hash(scw_list)

        file_name = f'scw_list_{scw_list_hash}'

        with open('scw_list_files/' + file_name, 'w+') as outlist_file:
            if format == 'string':
                outlist_file.write(",".join(scw_list))
            elif format == 'spaced_string':
                outlist_file.write(" ".join(scw_list))
            elif format == 'list':
                for scw in scw_list:
                    outlist_file.write(str(scw) + '\n')
        return f'scw_list_files/{file_name}'

    @staticmethod
    def create_catalog_object(wrong_format=False):
        selected_catalog = "{\"cat_column_descr\":[[\"meta_ID\",\"<i8\"],[\"src_names\",\"<U20\"],[\"significance\",\">f4\"],[\"ra\",\">f4\"],[\"dec\",\">f4\"],[\"NEW_SOURCE\",\">i2\"],[\"ISGRI_FLAG\",\"<i8\"],[\"FLAG\",\"<i8\"],[\"ERR_RAD\",\"<f8\"]],\"cat_column_list\":[[1,2,3,4,5,6,7,8,9],[\"1E 1740.7-2942\",\"4U 1700-377\",\"GRS 1758-258\",\"GX 1+4\",\"GX 354-0\",\"GX 5-1\",\"IGR J17252-3616\",\"SLX 1735-269\",\"Swift J1658.2-4242\"],[50.481285095214844,29.631359100341797,39.41709899902344,19.39865493774414,17.236827850341797,10.458189964294434,7.3749494552612305,8.645143508911133,8.171965599060059],[265.97705078125,255.96563720703125,270.2925720214844,263.0119934082031,263.00067138671875,270.2991027832031,261.33197021484375,264.5558166503906,254.55958557128906],[-29.746740341186523,-37.84686279296875,-25.736726760864258,-24.74085235595703,-33.82389831542969,-25.082794189453125,-36.24260330200195,-27.056262969970703,-42.71879196166992],[-32768,-32768,-32768,-32768,-32768,-32768,-32768,-32768,-32768],[2,2,2,2,2,2,1,2,2],[0,0,0,0,0,0,0,0,0],[0.000029999999242136255,0.0002800000074785203,0.0002800000074785203,0.0002800000074785203,0.0002800000074785203,0.0008299999753944576,0.0011099999537691474,0.00016999999934341758,0.00005555555617320351]],\"cat_column_names\":[\"meta_ID\",\"src_names\",\"significance\",\"ra\",\"dec\",\"NEW_SOURCE\",\"ISGRI_FLAG\",\"FLAG\",\"ERR_RAD\"],\"cat_coord_units\":\"deg\",\"cat_frame\":\"fk5\",\"cat_lat_name\":\"dec\",\"cat_lon_name\":\"ra\"}"

        selected_catalog_obj = json.loads(selected_catalog)

        if wrong_format:
            selected_catalog_obj['cat_column_list'][8].append(0)

        return selected_catalog_obj

    @staticmethod
    def create_catalog_file(catalog_value, wrong_format=False):
        # generate ScWs list file
        if not os.path.exists('catalog_simple_files'):
            os.makedirs('catalog_simple_files')

        # hash file content
        catalog_hash = make_hash(catalog_value)

        file_name = f'catalog_{catalog_hash}.txt'

        catalog_str = (
                """# %ECSV 0.9
                # ---
                # datatype:
                # - {name: meta_ID, datatype: int64}
                # - {name: src_names, datatype: string}
                # - {name: significance, datatype: float32}
                # - {name: ra, datatype: float32}
                # - {name: dec, datatype: float32}
                # - {name: NEW_SOURCE, datatype: uint16}
                # - {name: ISGRI_FLAG, datatype: int64}
                # - {name: FLAG, datatype: int64}
                # - {name: ERR_RAD, datatype: float64}
                # meta: !!omap
                # - {FRAME: fk5}
                # - {LAT_NAME: dec}
                # - {COORD_UNIT: deg}
                # - {LON_NAME: ra}
                # schema: astropy-2.0
                meta_ID src_names significance ra dec NEW_SOURCE ISGRI_FLAG FLAG ERR_RAD
                0 "1E 1740.7-2942" 50.4813 265.9771 -29.7467 -32768 2 0 0.0000345
                """)
        if wrong_format:
            catalog_str += " 0000"
        with open('catalog_simple_files/' + file_name, 'w+') as outlist_file:
            outlist_file.write(catalog_str)

        return f'catalog_simple_files/{file_name}'



    @classmethod
    def from_run_analysis_response(cls, r):
        return cls(
            session_id=r['session_id'],
            job_id=r['job_monitor']['job_id']
        )

    def __init__(self, session_id, job_id) -> None:
        self.session_id = session_id
        self.job_id = job_id
    
    @property
    def scratch_dir(self):
        return glob.glob(f'scratch_sid_{self.session_id}_jid_{self.job_id}*')[0]

    @property
    def job_monitor_json_fn(self):
        job_monitor_json_fn = f'{self.scratch_dir}/job_monitor.json'
        assert os.path.exists(job_monitor_json_fn) 

        return job_monitor_json_fn

    @property
    def email_history_folder(self) -> str:
        return f'{self.scratch_dir}/email_history'

    def assert_email(self, state, number=1, comment=""):
        list_email_files = glob.glob(self.email_history_folder + f'/email_{state}_*.email')
        assert len(list_email_files) == number, f"expected {number} emails, found {len(list_email_files)}: {list_email_files} in {self.email_history_folder}; {comment}"

    def load_job_state_record(self, state, message):
        return json.load(open(f'{self.scratch_dir}/job_monitor_{state}_{message}_.json'))

    def load_emails(self):
        return [ open(fn).read() for fn in glob.glob(f"{self.email_history_folder}/*.email")]