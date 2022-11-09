from flask.json import JSONEncoder
import logging
import numpy as np

from oda_api.data_products import NumpyDataProduct, ODAAstropyTable
from ..configurer import ConfigEnv

from astropy.io.fits.card import Undefined as astropyUndefined

class CustomJSONEncoder(JSONEncoder):

    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return list(obj)

        if isinstance(obj, astropyUndefined):
            return "UNDEFINED"
        
        if isinstance(obj, (NumpyDataProduct, ODAAstropyTable)):
            return obj.encode()

        if isinstance(obj, ConfigEnv):
            return obj.as_dict()
        
        if isinstance(obj, bytes):
            return obj.decode()

        logging.error("problem encoding %s, will NOT send as string", obj) # TODO: dangerous probably, fix!
        raise RuntimeError('unencodable ' + str(obj))

        #return JSONEncoder.default(self, obj)
