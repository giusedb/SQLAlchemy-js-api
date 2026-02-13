from datetime import date

import pytest
from sqlalchemy import String, ForeignKey, Integer
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.testing.schema import mapped_column

from src.jsalchemy_api import ResourceManager, DBResource
from jsalchemy_web_context import session

@pytest.mark.asyncio
async def test_login(context, auth, base_users):
    rm = ResourceManager(auth, context)
    result = await rm.login('alice', 'alice')
    assert result
    assert type(result) == dict
    assert 'token' in result
    assert 'user_id' in result

    token = result['token']
    user_id = result['user_id']

    async with context(token):
        assert session.user_id == user_id


# @pytest.mark.asyncio
def test_describe(Base, auth, context, all_types):
    rm = ResourceManager(auth, context)

    resource = DBResource(rm, 'AllTypes', all_types, desc='all types are in this table',
                          format_string='${this.string}')

    desc = resource.description
    assert desc
    assert len({'name', 'description', 'fields', '$pk', 'references', 'format_string', 'verbs', 'rpp'}.difference(desc.keys())) == 0
    assert desc['description'] == 'all types are in this table'
    assert desc['$pk'] == ['id']
    assert desc['references'] == []
    assert desc['format_string'] == '${this.string}'
    assert desc['verbs'] == []
    fields = {d['name']: d for d in desc['fields']}
    assert set(fields) == {'clob', 'integer', 'string', 'blob', 'big_integer', 'double', 'flt', 'boolean', 'interval',
                           'large_binary', 'obj', 'small_int', 'tuple_type', 'uuid', 'json', 'id'}
    assert fields['clob']['description'] == 'this contains a long text'
    assert fields['integer']['description'] == None

    assert fields['clob']['type'] == 'String'
    assert fields['integer']['type'] == 'Integer'
    assert fields['string']['type'] == 'String'
    assert fields['blob']['type'] == 'String'
    assert fields['big_integer']['type'] == 'Integer'
    assert fields['double']['type'] == 'Float'
    assert fields['flt']['type'] == 'Float'
    assert fields['boolean']['type'] == 'Boolean'
    assert fields['interval']['type'] == 'Interval'
    assert fields['large_binary']['type'] == 'String'
    assert fields['obj']['type'] == 'Object'
    assert fields['small_int']['type'] == 'Integer'
    assert fields['tuple_type']['type'] == 'Array'
    assert fields['uuid']['type'] == 'String'
    assert fields['json']['type'] == 'Object'

    assert fields['clob']['description'] == 'this contains a long text'

def test_references(Base, auth, context, all_types):

    class User(Base):
        __tablename__ = 'user'
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(150), nullable=False)
        type_id: Mapped[int] = mapped_column(ForeignKey('all_types.id'))
        type: Mapped[all_types] = relationship(all_types, backref='user')

    rm = ResourceManager(auth, context)

    resource = DBResource(rm, 'AllTypes', all_types)

    assert list(resource.references) == []

    user_resource = DBResource(rm, 'User', User)
    first_reference = next(resource.references)
    assert first_reference['resource'] == 'User'
    assert first_reference['type'] == 'many'