
class APIerror(Exception):

    def __init__(self, message, status_code=None, payload=None):
        super().__init__()
        self.message = message

        if status_code is not None:
            self.status_code = status_code
        else:
            self.status_code = 400

        self.payload = payload
        print(f'APIerror {self} constructor: {message}')

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['error_message'] = self.message
        return rv

    def __str__(self):
        return repr(self)

class BadRequest(APIerror):
    def __init__(self, message, status_code=None, payload=None):
        super().__init__(message, status_code=400, payload=payload)

class RequestNotUnderstood(BadRequest):
    """
    positive exception messages only!
    it is not user error
    it is not bad request
    it is unclear for us and we want user to please clarify!
    """

class RequestNotAuthorized(BadRequest):
    pass

class MissingParameter(RequestNotUnderstood):
    pass

###

class UnfortunateRequestResults(Exception):
    """
    positive exception messages only!
    it is not user error
    it is not bad request
    it is unclear for us and we want user to please clarify!
    """

class ProblemDecodingStoredQueryOut(Exception):
    """
    problem with storage? race?
    """
