import pytest

import cdci_data_analysis.flask_app.app
from cdci_data_analysis.configurer import ConfigEnv

import os
import re

__this_dir__ = os.path.join(os.path.abspath(os.path.dirname(__file__)))

import signal, psutil
def kill_child_processes(parent_pid, sig=signal.SIGTERM):
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

    class CustomHandler:
        async def handle_DATA(self, server, session, envelope):
            try:
                peer = session.peer
                mail_from = envelope.mail_from
                rcpt_tos = envelope.rcpt_tos
                data = envelope.content
                print(f"mail server: Receiving message from: {peer}")
                print(f"mail server: Message addressed from: {mail_from}")
                print(f"mail server: Message addressed to: {rcpt_tos}")
                print(f"mail server: Message length : {len(data)}")
            except:
                return '500 Could not process your message'
            return '250 OK'

    handler = CustomHandler()

    controller = Controller(handler, hostname='127.0.0.1', port=1025)
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
    kill_child_processes(p.pid, signal.SIGKILL)
    os.kill(p.pid, signal.SIGKILL)


@pytest.fixture
def dispatcher_live_fixture(pytestconfig):
    import subprocess
    import os
    import copy
    import time
    from threading import Thread

    env = copy.deepcopy(dict(os.environ))
    print(("rootdir", str(pytestconfig.rootdir)))
    env['PYTHONPATH'] = str(pytestconfig.rootdir) + ":" + str(pytestconfig.rootdir) + "/tests:" + env.get('PYTHONPATH', "")
    print(("pythonpath", env['PYTHONPATH']))
        
    cmd = [ 
            "python", 
            os.path.join(__this_dir__, "../bin/run_osa_cdci_server.py"),
            "-d",
            "-conf_file", os.path.join(__this_dir__, "../tests/test-conf.yaml"),
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
                print(("found url:", url_store[0]))

            if re.search("\* Debugger PIN:.*?", line):
                url_store[0] = url_store[0].replace("0.0.0.0", "127.0.0.1")
                print("server ready, url", url_store[0])


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
    kill_child_processes(p.pid,signal.SIGKILL)
    os.kill(p.pid, signal.SIGKILL)

