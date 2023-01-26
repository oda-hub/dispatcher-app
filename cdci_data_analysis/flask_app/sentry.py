import logging
import sentry_sdk

logger = logging.getLogger(__name__)

class Sentry:
    def __init__(self) -> None:
        self._app = None

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
                    max_breadcrumbs=50,
                )
            except Exception as e:
                logger.warning("can not setup sentry with URL %s due to %s", self.sentry_url, e)

            return True

    def capture_message(self, message: str, logger=None):
        if self.have_sentry:
            if logger is not None:
                logger.warning(message)

            sentry_sdk.capture_message(message)
        else:
            logger.warning("sentry not used, dropping %s", message)

sentry = Sentry()
