from celery import Celery
from celery.result import AsyncResult
import typing
import requests

import os
import time

def make_celery():
    celery = Celery(
        'dispatcher',
        broker=os.environ.get("CELERY_BROKER", "redis://localhost:6379"),
        backend=os.environ.get("CELERY_BACKEND", "redis://localhost:6379"),
    )
    return celery

celery = make_celery()
celery.conf.result_expires = 600

@celery.task()
def request_dispatcher(url, **params):
    print("\033[31mquery URL", url, "\033[0m")
    print("\033[31mquery params", params, "\033[0m")

    r = None
    for i in reversed(range(10)):
        try:
            r = requests.get(url, params=params)
            break
        except Exception as e:
            print("\033[31m exception in the request", e, "\033[0m")
            if i == 0:
                raise
            else:
                time.sleep(1 + int(i**0.5))

    print("\033[35m", r.text[:200], "\033[0m")

    return url + str(params)

def flower_task(task_id: str) -> typing.Union[None, dict]:
    # same pod, port is fixed
    r = requests.get(os.environ.get("CELERY_FLOWER", "http://localhost:5555") +
                     "/api/task/info/" + task_id)

    if r.status_code == 404:
        return None

    try:
        return r.json()
    except Exception as e:
        # log and complan here TODO
        raise

