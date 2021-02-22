from flask.json import JSONEncoder
import numpy as np

from astropy.io.fits.card import Undefined as astropyUndefined

class CustomJSONEncoder(JSONEncoder):

    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return list(obj)

        if isinstance(obj, astropyUndefined):
            return "UNDEFINED"

        logging.error("problem encoding %s, will send as string", obj) # TODO: dangerous probably, fix!
        return 'unencodable ' + str(obj)

        #return JSONEncoder.default(self, obj)
