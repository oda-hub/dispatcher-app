from flask.json import JSONEncoder
import logging
import numpy as np

from oda_api.data_products import NumpyDataProduct
from ..configurer import ConfigEnv

from astropy.io.fits.card import Undefined as astropyUndefined

class CustomJSONEncoder(JSONEncoder):

    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return list(obj)

        if isinstance(obj, astropyUndefined):
            return "UNDEFINED"
        
        if isinstance(obj, NumpyDataProduct):
            return obj.encode()

        if isinstance(obj, ConfigEnv):
            return obj.as_dict()

        logging.error("problem encoding %s, will send as string", obj) # TODO: dangerous probably, fix!
        return 'unencodable ' + str(obj)

        #return JSONEncoder.default(self, obj)
