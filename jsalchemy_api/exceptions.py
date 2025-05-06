
class JSAlchemyException(Exception):
    """Helps the HTTP exception compute flows."""

    status_code = 500
    message = 'Internal Server Error'

    def __init__(self, status_code=None, message=None):
        if status_code:
            self.status_code = status_code
        if message:
            self.message = message

class ResourceNotFoundException(JSAlchemyException):
    """One of the web resource wasn't found."""
    status_code = 404


class SessionNotFound(JSAlchemyException):

    def __init__(self, token):
        super().__init__(403, f'Session {token} not found')


class RecordNotFound(JSAlchemyException):
    def __init__(self, message):
        super().__init__(404, message)