# this could be a separate package or/and a pytest plugin

from json import JSONDecodeError

import pytest

import cdci_data_analysis.flask_app.app
from cdci_data_analysis.configurer import ConfigEnv

import os
import re
import json
import string
import random

__this_dir__ = os.path.join(os.path.abspath(os.path.dirname(__file__)))

import signal, psutil
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
def dispatcher_local_mail_server(pytestconfig):
    from aiosmtpd.controller import Controller

    class CustomController(Controller):
        def __init__(self, id, handler, hostname='127.0.0.1', port=61025):
            self.id = id
            super().__init__(handler, hostname=hostname, port=port)


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
    handler = CustomHandler(f'local_smtp_log/{id}_local_smtp_output.json')
    controller = CustomController(id, handler, hostname='127.0.0.1', port=1025)
    # Run the event loop in a separate thread
    controller.start()

    yield controller

    print("will stop the mail server")
    controller.stop()


@pytest.fixture
def dispatcher_local_mail_server_subprocess(pytestconfig):
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
        " -m smtpd",
        "-c DebuggingServer",
        "-n localhost:1025"
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
def dispatcher_test_conf(tmpdir):
    fn = os.path.join(tmpdir, "test-dispatcher-conf.yaml")
    with open(fn, "w") as f:
        f.write("""
dispatcher:
    dummy_cache: dummy-cache
    products_url: http://www.astro.unige.ch/cdci/astrooda_
    dispatcher_url: 0.0.0.0
    dispatcher_port: 8001
    dispatcher_service_url: http://localhost:8001
    sentry_url: "https://2ba7e5918358439485632251fa73658c@sentry.io/1467382"
    logstash_host: 10.194.169.75
    logstash_port: 5001
    secret_key: 'secretkey_test'
    email_options:
        smtp_server: 'localhost'
        sender_mail: 'team@odahub.io'
        cc_receivers_mail: ['team@odahub.io']
        smtp_port: 1025
        smtp_server_password: ''
        email_sending_timeout: True
        email_sending_timeout_default_threshold: 1800
        email_sending_job_submitted: True
    """)

    yield fn

@pytest.fixture
def dispatcher_live_fixture(pytestconfig, dispatcher_test_conf):
    import subprocess
    import os
    import copy
    import time
    from threading import Thread

    env = copy.deepcopy(dict(os.environ))
    print(("rootdir", str(pytestconfig.rootdir)))
    env['PYTHONPATH'] = str(pytestconfig.rootdir) + ":" + str(pytestconfig.rootdir) + "/tests:" + env.get('PYTHONPATH', "")
    print(("pythonpath", env['PYTHONPATH']))

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
            "-conf_file", dispatcher_test_conf,
            "-debug",
            #"-use_gunicorn" should not be used, as current implementation of follow_output is specific to flask development server
          ] 

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
            m = re.search(r"Running on (.*?) \(Press CTRL\+C to quit\)", line)
            if m:
                url_store[0] = m.group(1)[:-1]  # alaternatively get from configenv
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

    service=url_store[0]

    yield service

    
    print("will keep service alive a bit, for async")
    time.sleep(0.5)

    print(("child:",p.pid))
    import os,signal
    kill_child_processes(p.pid,signal.SIGINT)
    os.kill(p.pid, signal.SIGINT)


