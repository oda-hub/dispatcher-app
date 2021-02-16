import subprocess
import requests
import time
import re
import signal
import os
import traceback

from threading import Thread
from time import sleep

import pytest

__this_dir__ = os.path.join(os.path.abspath(os.path.dirname(__file__)))

#pytestmark = pytest.mark.skip("these tests still WIP")

class DispatcherServer(object):
    def __init__(self):
        pass

    url=None

    def follow_output(self):
        url=None
        for line_b in iter(self.process.stdout.readline, b''):
            line = line_b.decode()

            print("following server:", line.rstrip())

            m = re.search("Running on (.*?) \(Press CTRL\+C to quit\)", line)
            if m:
                self.url = m.group(1).replace("0.0.0.0", "127.0.0.1") # alternatively get from configenv
                print("found url:", self.url)

            if re.search("\* Debugger PIN:.*?", line) or \
               re.search("Debug mode: off", line):
                print("server ready")

    def start(self):
        cmd = ["python", __this_dir__+"/../bin/run_osa_cdci_server.py"]
        print("command:"," ".join(cmd))
        self.process=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,shell=False)

        print("\n\nfollowing server startup")

        thread = Thread(target = self.follow_output, args = ())
        thread.start()

        while self.url is None:
            time.sleep(0.1)
        time.sleep(0.5)

        # this is patch?
        #self.url="http://127.0.0.1:5000"

        return self
    

    def stop(self):
        pass
        os.kill(os.getpgid(self.process.pid), signal.SIGTERM)
        #os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)

    def __enter__(self):
        return self.start()

    def __exit__(self, _type, value, tracebac):
        print(("exiting:",_type,value, tracebac))
        traceback.print_tb(tracebac)
        time.sleep(0.5)
        self.stop()

def test_no_instrument():
    with DispatcherServer() as server:
        print("constructed server:", server)

        c=requests.get(server.url + "run_analysis",
                       params=dict(
                       image_type="Real",
                       product_type="image",
                       E1=20.,
                       E2=40.,
                       T1="2008-01-01T11:11:11.0",
                       T2="2008-06-01T11:11:11.0",
                    ))

        print("content:", c.text)

        jdata=c.json()

        assert c.status_code == 400


def test_isgri_image_instrument():
    with DispatcherServer() as server: # this should be fixture
        print("constructed server:", server)

        c=requests.get(server.url + "run_analysis",
                       params=dict(
                           image_type="Real",
                           product_type="image",
                           E1=20.,
                           E2=40.,
                           T1="2008-01-01T11:11:11.0",
                           T2="2008-06-01T11:11:11.0",
                        )
                      )

        print("content:", c.text)

        jdata=c.json()
        print('done')
        print(list(jdata.keys()))
        print(jdata['data'])


