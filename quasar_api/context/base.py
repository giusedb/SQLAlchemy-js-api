import contextvars
from contextvars import ContextVar


class Session:

    def __getitem__(self, item):
        """Get `item` from the current Session's object."""
        raise NotImplementedError

    def __setitem__(self, key, value):
        """Save `item` in the current Session."""
        raise NotImplementedError

    def __delitem__(self, key):
        """Delete `key` from the current Session's object.'"""
        raise NotImplementedError

    def __iter__(self):
        """Iterates through all sessions"""
        raise NotImplementedError

    def __contains__(self, item):
        """Check if `item` exists in the current Session's object.'"""
        raise NotImplementedError

    def __len__(self):
        """Gets the total count of all sessions."""
        raise NotImplementedError

    def __pickle__(self):
        """Serialize the session"""
        raise NotImplementedError


class SessionManager:

    def connect(self, token):
        """Connects current session object to the current request's token."""
        raise NotImplementedError

    def disconnect(self, session: Session):
        """Disconnects current session object and store the object."""
        raise NotImplementedError

    def new(self):
        """Generate a new session object."""
        raise NotImplementedError



class ContextAttribute:
    __slots__ = ('name', 'var')

    def __init__(self, name: str):
        self.name = name
        self.var = ContextVar(name)

    def __get__(self, instance, owner):
        return self.var.get()

    def __set__(self, instance, value):
        self.var.set(value)


class FixedContext(type):

    def __new__(cls, c_name, bases, attrs):
        vars = attrs.get('vars', ())
        for name in vars:
            attrs[name] = ContextAttribute(name)
        instance = super().__new__(cls, c_name, bases, attrs)
        return instance


class FixedContextManger(metaclass=FixedContext):
    pass


if __name__ == '__main__':
    contextvars.Context()