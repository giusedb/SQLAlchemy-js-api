from typing import List

from jsalchemy_api import DBResource, ResourceManager
from jsalchemy_api.resources.base import verb
from jsalchemy_api.examples.empty.modules.base.auth import User


class UserResource(DBResource):

    def __init__(self, resource_manager: ResourceManager):
        super().__init__(
            resource_manager, 'User',User)

    @verb
    async def post(self):
        raise NotImplementedError()

    @verb
    async def delete(self, pks: List[str]) -> None:
        raise NotImplementedError()


    @verb(name='cippa')
    async def cippa(self, instance: User):
        return {
            'last_name': instance.last_name
        }
