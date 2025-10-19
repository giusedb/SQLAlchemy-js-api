from collections import defaultdict
from itertools import groupby
from typing import List, Dict, Any, Optional, Type

from sqlalchemy import event, inspect
from sqlalchemy.orm import DeclarativeBase, Session, RelationshipDirection
from sqlalchemy.orm.unitofwork import UOWTransaction

from jsalchemy_api.resources.base import ResultData
from jsalchemy_api.utils import dict_diff
from jsalchemy_web_context import request


class ChangeInterceptor:
    def __init__(self, resource_manager: 'ResourceManager'):
        """
        Alternative implementation with better session management.

        Args:
            model_classes: List of model classes to track changes for.
        """
        self.rm = resource_manager
        self._session_trackers: Dict[Session, dict] = {}
        self.register_session(Session)

    def start_record(self):
        request.tracker = ResultData()
        request.loaded = defaultdict(list)

    @property
    def changes(self):
        return request.tracker

    @property
    def new(self):
        items = self.changes['new']
        return {tp.__name__: list(map(self.rm.resources[tp].serialize, grp))
                for tp, grp in groupby(sorted(items, key=lambda x: type(x).__name__), type)}

    @property
    def updated(self):
        ret = defaultdict(list)
        loaded = {(cls, item['id']): item for cls, items in request.loaded.items() for item in items}
        for item in self.changes['updated']:
            model_name = type(item).__name__
            serialized = self.rm.resources[type(item)].serialize(item)
            previous = loaded.get((model_name, item.id))
            if not previous:
                continue
            diff = dict_diff(previous, serialized)
            if diff:
                diff['id'] = item.id
                ret[model_name].append(diff)
        return dict(ret)

    @property
    def deleted(self):
        return {tp.__name__: [x.id for x in grp]
                for tp, grp in groupby(sorted(self.changes['deleted'], key=lambda x: type(x).__name__), type)}

    @property
    def m2m(self):
        return []

    def register_session(self, session: Session):
        """Register a session for change tracking."""
        # Register event listeners for this session
        event.listen(session, 'before_commit', self._on_after_commit)
        event.listen(session, 'before_flush', self._on_before_flush)

    def _on_before_flush(self, session: Session, transaction: UOWTransaction, *args):
        """Track changes before any flush."""
        tracker = request.result
        if session._new:
            tracker.new.update(set(session.new))
        if session._deleted:
            for item in session.deleted:
                titem = type(item)
                pk = item.id
                tracker['deleted'].setdefault(titem.__name__, set()).add(pk)
        if session.dirty:
            tracker.update.update(session.dirty)

    def _on_after_commit(self, session):
        """Store changes in the tracker."""
        print('--> send changes <--')

    def _on_before_commit(self, session):
        """Store changes in the tracker."""
        print('--> before commit <--')
        request.results = {
            'new': self.new,
            'updated': self.updated,
            'deleted': self.deleted,
            'm2m': self.m2m,
        }

    def _load_model(self, target: DeclarativeBase, context):
        """store the object footprint when the object is saved from database."""
        resource = self.rm.resources[target.__class__]
        request.loaded[target.__class__.__name__].append(resource.serialize(target))

    def _m2m_append(self, target, value, initiator):
        """update the m2m on the session tracker."""
        self.changes['m2m'].append(('add', type(value).__name__, initiator.key, [value.id, target.id]))

    def _m2m_remove(self, target, value, initiator):
        """update the m2m on the session tracker."""
        self.changes['m2m'].append(('del', type(value).__name__, initiator.key, [value.id, target.id]))

    def register_model(self, model):
        """Check all the M2M properties and connects the interceptor to them."""
        event.listen(model, 'load', self._load_model)
        m2ms = (prop for prop in model.__mapper__.relationships if
                prop.direction == RelationshipDirection.MANYTOMANY)

        for m2m in m2ms:
            event.listen(m2m, 'append', self._m2m_append)
            event.listen(m2m, 'remove', self._m2m_remove)

