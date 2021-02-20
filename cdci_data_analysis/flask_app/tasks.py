from celery import Celery
import requests

def make_celery():
    celery = Celery(
        'dispatcher',
        broker='redis://localhost:6379',
        backend='redis://localhost:6379',
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

    requests.get(url, params=params)

    return url + str(params)
