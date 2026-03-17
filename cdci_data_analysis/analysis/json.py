from oda_api.json import CustomJSONEncoder as APICustomJSONEncoder
from flask.json.provider import DefaultJSONProvider
import json
from ..configurer import ConfigEnv

class CustomJSONEncoder(APICustomJSONEncoder):

    def default(self, obj):
        if isinstance(obj, ConfigEnv):
            return obj.as_dict()
        
        return super().default(obj)
    
class CustomJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs) -> str:
        kwargs['cls'] = CustomJSONEncoder
        return json.dumps(obj, **kwargs)
