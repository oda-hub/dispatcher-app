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
    app.config['conf'] = ConfigEnv.from_conf_file("cdci_data_analysis/config_dir/conf_env.yml")
    return app

@pytest.yield_fixture
def dispatcher_live_fixture(pytestconfig):
    import subprocess
    import os
    import copy
    import time
    from threading import Thread

    env=copy.deepcopy(dict(os.environ))
    print(("rootdir",str(pytestconfig.rootdir)))
    env['PYTHONPATH'] = str(pytestconfig.rootdir)+":"+str(pytestconfig.rootdir)+"/tests:"+env.get('PYTHONPATH', "")
    print(("pythonpath",env['PYTHONPATH']))
        
    cmd = ["python", __this_dir__+"/../bin/run_osa_cdci_server.py"]

    p=subprocess.Popen(
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
            print("following server:", line.rstrip())
            m = re.search(b"Running on (.*?) \(Press CTRL\+C to quit\)", line)
            if m:
                url_store[0] = m.group(1)[:-1]  # alaternatively get from configenv
                print(("found url:", url_store[0]))

            if re.search(b"\* Debugger PIN:.*?", line):
                url_store[0] = url_store[0].replace("0.0.0.0", "127.0.0.1")
                print(("server ready, url", url_store[0]))


    thread = Thread(target=follow_output, args=())
    thread.start()

    while url_store[0] is None:
        time.sleep(0.1)
    time.sleep(0.5)

    service=url_store[0]

    yield service

    print(("child:",p.pid))

    import os,signal
    kill_child_processes(p.pid,signal.SIGKILL)
    os.kill(p.pid, signal.SIGKILL)

