class APIerror(Exception):

    def __init__(self, message, status_code=None, payload=None):
        super().__init__(self)
        self.message = message

        if status_code is not None:
            self.status_code = status_code
        self.payload = payload
        print('API Error Message constructor:', message)

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['error_message'] = self.message
        return rv

class BadRequest(APIerror):
    def __init__(self, message, status_code=None, payload=None):
        super().__init__(message, status_code=400, payload=payload)
