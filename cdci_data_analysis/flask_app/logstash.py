import time
import json
import logging
import pylogstash

logger = logging.getLogger(__name__)

def logstash_message(app, message_dict: dict):
    t0 = time.time()

    logger.warning("logstash_message at %s s", t0)
    conf = app.config['conf']

    logger.warning("logstash_message before encoding %s in %s s", t0, time.time() - t0)
    message_json = json.dumps(message_dict)
    logger.warning("logstash_message after encoding %s size %s in %s s", t0, len(message_json), time.time() - t0)    
    
    if conf.logstash_host not in [None, "None"] and conf.logstash_port not in [None, "None"]:
        url = f"{conf.logstash_host}:{conf.logstash_port}"

        LS = pylogstash.LogStasher(url)

        logger.warning("logstash_message constructed at %s in %s s", t0, time.time() - t0)

        LS.log(
            message_dict
        )

        logger.warning("logstash_message sent at %s in %s s", t0, time.time() - t0)

    logger.debug(f"\033[35m stashing to {conf.logstash_host}:{conf.logstash_port}\033[0m")
    logger.debug("\033[35m%s\033[0m", message_dict)

    logger.warning("logstash_message done at %s in %s s", t0, time.time() - t0)
    
