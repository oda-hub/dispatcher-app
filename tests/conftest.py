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
            "-conf_file", os.path.join(__this_dir__, "../tests/test-conf.yaml"),
            "-use_gunicorn",
            "-debug"
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

