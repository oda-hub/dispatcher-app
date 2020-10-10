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

pytestmark = pytest.mark.skip("these tests still WIP")

class DispatcherServer(object):
    def __init__(self):
        pass

    url=None

    def follow_output(self):
        url=None
        for line in iter(self.process.stdout.readline,''):
            print("following server:",line.rstrip())
            m=re.search("Running on (.*?) \(Press CTRL\+C to quit\)",line)
            if m:
                url=m.group(1) # alaternatively get from configenv
                print(("found url:",url))
        
            if re.search("\* Debugger PIN:.*?",line):
                print("server ready")
                url=url.replace("0.0.0.0","127.0.0.1")
                self.url=url

    def start(self):
        cmd=["python",__this_dir__+"/../bin/run_osa_cdci_server.py"]
        print(("command:"," ".join(cmd)))
        self.process=subprocess.Popen(cmd,stdout=subprocess.PIPE, stderr=subprocess.STDOUT,shell=False)

        print("\n\nfollowing server startup")

        thread = Thread(target = self.follow_output, args = ())
        thread.start()

        while self.url is None:
            time.sleep(0.1)
        time.sleep(0.5)

        self.url="http://127.0.0.1:5000"

        return self
    

    def stop(self):
        os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)

    def __enter__(self):
        return self.start()

    def __exit__(self, _type, value, tracebac):
        print(("exiting:",_type,value, tracebac))
        traceback.print_tb(tracebac)
        time.sleep(0.5)
        self.stop()

def test_urltest():
    with DispatcherServer() as server:
        print(server)
        c=requests.get(server.url+"/test",params=dict(
                        image_type="Real",
                        product_type="image",
                        E1=20.,
                        E2=40.,
                        T1="2008-01-01T11:11:11.0",
                        T2="2008-06-01T11:11:11.0",
                    ))
        jdata=c.json()
        print('done')
        print(list(jdata.keys()))
        print(jdata['data'])


