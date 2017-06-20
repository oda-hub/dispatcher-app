import subprocess
import requests
import time
import re
import signal
import os

class DispatcherServer(object):
    def __init__(self):
        pass

    def start(self):
        self.process=subprocess.Popen(["python","bin/run_osa_cdci_server.py"],stdout=subprocess.PIPE, stderr=subprocess.PIPE) # separate process or thread?.. 

        print("\n\nfollowing server startup")
        for line in iter(self.process.stderr.readline,''):
            print line.rstrip()
            m=re.search("Running on (.*?) \(Press CTRL\+C to quit\)",line)
            if m:
                self.url=m.group(1)
                print("found url:",self.url)
        
            if re.search("\* Debugger PIN:.*?",line):
                print("server ready")
                break


        return self

    def stop(self):
        os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)

    def __enter__(self):
        return self.start()

    def __exit__(self, _type, value, traceback):
        print("exiting:",_type,value, traceback)
        self.stop()

def test_starting():
    with DispatcherServer() as server:
        print server
        c=requests.get(server.url+"/")
        print c.content
