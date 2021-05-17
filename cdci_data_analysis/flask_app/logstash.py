import logging
import PyLogStash

logger = logging.getLogger(__name__)

def logstash_message(app, message_json: dict):
    conf = app.config['conf']

    if conf.logstash_host not in [None, "None"] and conf.logstash_port not in [None, "None"]:
        url = f"{conf.logstash_host}:{conf.logstash_port}"

        PyLogStash.LogStasher(url).log(
            message_json
        )

    logger.debug(f"\033[35m stashing to {conf.logstash_host}:{conf.logstash_port}\033[0m")
    logger.debug(f"\033[35m{message_json}\033[0m")
    
