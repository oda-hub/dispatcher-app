import logging
import sentry_sdk

# logger = logging.getLogger(__name__)

class Sentry:
    def __init__(self) -> None:
        self._app = None
        self.logger = logging.getLogger(repr(self))

    @property
    def app(self):
        if self._app:
            return self._app
        else:
            raise RuntimeError

    @app.setter
    def app(self, app):
        self._app = app

    @property
    def sentry_url(self):
        if not hasattr(self, '_sentry_url'):
            self._sentry_url = getattr(self.app.config.get('conf'), 'sentry_url', None)
        
        return self._sentry_url

    @property
    def have_sentry(self):
        if self.sentry_url is None:
            return False
        else:
            try:
                sentry_sdk.init(
                    dsn=self._sentry_url,
                    # Set traces_sample_rate to 1.0 to capture 100%
                    # of transactions for performance monitoring.
                    # We recommend adjusting this value in production.
                    traces_sample_rate=0.1,
                    debug=False,
                    max_breadcrumbs=10,
                    environment=getattr(self.app.config.get('conf'), 'sentry_environment', 'production'),
                    before_send=self.filter_event
                )
            except Exception as e:
                self.logger.warning("can not setup sentry with URL %s due to %s", self.sentry_url, e)

            return True

    def capture_message(self, message: str):
        if self.have_sentry:
            self.logger.warning(message)

            sentry_sdk.capture_message(message)
        else:
            self.logger.warning("sentry not used, dropping %s", message)

    @staticmethod
    def filter_event(event, hint):
        message = event.get("message", None)
        if message is None:
            log_record = hint.get("log_record", None)
            if log_record is not None:
                message = log_record.getMessage()

        if message is not None:
            if "AnalysisError" in message:
                return None

        return event


sentry = Sentry()
