from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from . import BaseModel
from jsalchemy_auth.models import UserMixin
from jsalchemy_authentication.mixins import IdentityMixin


class User(UserMixin, BaseModel):
    __tablename__ = 'users'
    first_name: Mapped[str]
    last_name: Mapped[str]
    nickname: Mapped[str] = mapped_column(String(), nullable=True)
    avatar: Mapped[str] = mapped_column(String(), nullable=True)
    email: Mapped[str] = mapped_column(String(), nullable=True)

    identities = relationship('Identity', back_populates='user')


class Identity(IdentityMixin, BaseModel):
    __tablename__ = 'identities'
    email: Mapped[str] = mapped_column(unique=True)
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'))

    user = relationship('User', back_populates='identities', foreign_keys=[user_id])

    def __repr__(self):
        return f'<Identity: {self.email} ({self.id})>'

    __str__ = __repr__


