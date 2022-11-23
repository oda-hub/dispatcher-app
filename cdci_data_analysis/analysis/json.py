from oda_api.json import CustomJSONEncoder as APICustomJSONEncoder
from ..configurer import ConfigEnv

class CustomJSONEncoder(APICustomJSONEncoder):

    def default(self, obj):
        if isinstance(obj, ConfigEnv):
            return obj.as_dict()
        
        return super().default(obj)
