from celery import Celery
from celery.result import AsyncResult
import requests

import os
import time

def make_celery():
    celery = Celery(
        'dispatcher',
        broker=os.environ.get("CELERY_BROKER", "redis://localhost:6379"),
        backend=os.environ.get("CELERY_BACKEND", "redis://localhost:6379"),
    )
    #celery.conf.update(app.config)

    # do we want that?
#    class ContextTask(celery.Task):
#        def __call__(self, *args, **kwargs):
#            with app.app_context():
#                return self.run(*args, **kwargs)

    #celery.Task = ContextTask
    return celery

celery = make_celery()

@celery.task()
def request_dispatcher(url, params):
    print("\033[31mquery URL", url, "\033[0m")
    print("\033[31mquery params", params, "\033[0m")

    for i in range(10):
        try:
            r = requests.get(url, params=params)
        except Exception as e:
            print("\033[31m exception in the request", e, "\033[0m")
            time.sleep(3)

    print("\033[35m", r.text[:200], "\033[0m")

    return url + str(params)
