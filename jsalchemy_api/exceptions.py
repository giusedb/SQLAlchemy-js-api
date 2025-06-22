
class JSAlchemyException(Exception):
    """Helps the HTTP exception compute flows."""

    status_code = 500
    message = 'Internal Server Error'

    def __init__(self, message: str=None, status_code: int=None):
        if status_code:
            self.status_code = status_code
        if message:
            self.message = message

class ResourceNotFoundException(JSAlchemyException):
    """One of the web resource wasn't found."""
    status_code = 404

class SessionNotFound(JSAlchemyException):

    def __init__(self, token):
        super().__init__(f'Session "{token}" not found', 403)


class RecordNotFound(JSAlchemyException):

    status_code = 404