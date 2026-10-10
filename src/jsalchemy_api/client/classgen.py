from typing import List, Dict, Any

from .base import ResourceDefinition, Resource, ReferenceDef
from .rset import RSet
from .utils import deep_diff


def make_resouce_class(resource_manager: 'jsa._resource_manager.JSAResourceManager', definition: ResourceDefinition) -> Resource:
    """Create a Python class dynamically to map the resource"""
    references = {field.attribute: field for field in definition.references }
    field_names = {field.name: field for field in definition.fields if field.name not in references}
    base_fields = {field.name: field for field in definition.fields }

    BASE_DICT = {k: None for k in base_fields}

    def create(cls, item: Dict[str, Any]) -> None:
        obj = cls(**item)
        obj._data.update(item)
        obj._is_new = False
        return obj

    def init(self, **kwargs) -> None:
        self._data = BASE_DICT.copy()
        for k, v in kwargs.items():
            setattr(self, k, v)
        self._is_new = True

    def update(self, data: Dict[str, Any]):
        self._data.update(data)
        for k, v in data.items():
            setattr(self, k, v)


    def resolve_direct(reference: ReferenceDef):
        def ref(self):
            """Get the reference to the resource."""
            collection = resource_manager.get_collection(reference.resource)
            if collection:
                return collection.get(getattr(self, reference.local_attribute))
            else:
                return Resource()
        return ref

    def resolve_many(reference: ReferenceDef):
        def ref(self):
            return RSet(resource_manager, reference.resource,
                        {reference.foreign_attribute: getattr(self, reference.local_attribute)})
        return ref

    def resolve_many_to_many(reference: ReferenceDef):
        def ref(self):
            return RSet(resource_manager, reference.resource,
                        {reference.foreign_attribute: getattr(self, reference.local_attribute)})
        return ref

    if len(definition.PK):
        _PK = definition.PK[0]
        def get_pk(self):
            # TODO: optimize this via compiled statement
            return str(getattr(self, _PK, None))
    else:
        def get_pk(self):
            # TODO: optimize this via compiled statement
            return ':'.join(str(getattr(self, x, None)) for x in definition.PK)

    def pk_from_dict(self, data: Dict[str, Any]) -> str:
        return ':'.join(str(data.get(x)) for x in definition.PK)

    def get(self, *pks: List[str], blocking: bool=False) -> List[Resource]:
        return self.__class__.collection.get(*pks, blocking=blocking)

    def to_dict(self):
        return { k: getattr(self, k) for k in base_fields }

    def diff(self):
        return deep_diff(self.RAW, self._data)

    def is_dirty(self):
        return bool(self.diff)

    def is_new(self):
        return not self.PK

    def save(self):
        nonlocal resource_manager
        if self._is_new:
            payload = resource_manager.verb(definition.name, 'post', self.RAW, skip=True)
            self.UPDATE(payload['new'][definition.name][0])
            collection = resource_manager.get_collection(definition.name)
            collection._index[self.PK] = self
            return
        diff = self.DIFF
        if diff:
            update = {k: new for k, (new, old) in diff.items()}
            update[definition.PK[0]] = self.PK
            resource_manager.verb(definition.name, 'put', update)

    attrs = dict(
        __init__=init,
        get_pk=classmethod(pk_from_dict),
        PK=property(get_pk),
        RPP=definition.rpp,
        COLLECTION=resource_manager.get_collection(definition.name),
        GET=get,
        RAW=property(to_dict),
        DIRTY=property(is_dirty),
        DIFF=property(diff),
        SAVE=save,
        UPDATE=update,
        create=classmethod(create),
        is_new=property(is_new),
    )
    attrs.update({name: None for name in base_fields})
    references = {}
    for reference in definition.references:
        if reference.type == 'one':
            references[reference.attribute] = property(resolve_direct(reference))
        elif reference.type == 'many':
            references[reference.attribute] = property(resolve_many(reference))
        elif reference.type == 'm2m':
            references[reference.attribute] = property(resolve_many_to_many(reference))
    attrs.update(references)

    return type(definition.name, (Resource,), attrs)
