from time import perf_counter
from contextlib import contextmanager
from cdci_data_analysis.flask_app.sentry import sentry

@contextmanager
def block_timer(logger=None,
                sentry=sentry,
                sentry_threshold=30,
                message_template="Execution took {:.2f} seconds"):
    t1 = t2 = perf_counter()
    try:
        yield lambda: t2 - t1 
    finally:
        t2 = perf_counter()
        message = message_template.format(t2-t1)
        if logger is not None:
            logger.info(message)
        if sentry is not None and t2-t1 > sentry_threshold:
            sentry.capture_message(message)
