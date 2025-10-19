from collections import defaultdict
from itertools import groupby
from typing import List, Dict, Any

from sqlalchemy import event
from sqlalchemy.orm import DeclarativeBase, Session, RelationshipDirection
from sqlalchemy.orm.unitofwork import UOWTransaction

from jsalchemy_api.resources.base import ResultData
from jsalchemy_api.utils import dict_diff
from jsalchemy_web_context import request


class ChangeInterceptor:
    """
    A class responsible for intercepting and tracking changes made to SQLAlchemy models
    during a session, including new, updated, and deleted entities.

    It integrates with the JSAlchemy API's resource manager to serialize objects
    and capture change information for use in tracking or reporting.
    """

    def __init__(self, resource_manager: 'ResourceManager'):
        """
        Initialize the ChangeInterceptor with a ResourceManager instance.

        Args:
            resource_manager (ResourceManager): The manager used to serialize
                                               model instances into data representations.
        """
        self.rm = resource_manager
        self._session_trackers: Dict[Session, dict] = {}
        self.register_session(Session)

    def start_record(self):
        """
        Initialize tracking of changes by setting up a ResultData object
        in the current request context.
        """
        request.tracker = ResultData()
        request.loaded = defaultdict(list)

    @property
    def changes(self) -> Dict[str, Any]:
        """
        Retrieve the current set of tracked changes from the request context.

        Returns:
            Dict[str, Any]: The full change tracking dictionary.
        """
        return request.tracker

    @property
    def new(self) -> Dict[str, List[Dict]]:
        """
        Retrieve all newly created entities grouped by model type.

        Returns:
            Dict[str, List[Dict]]: A mapping from model name to list of serialized new entities.
        """
        items = self.changes['new']
        return {
            tp.__name__: list(map(self.rm.resources[tp].serialize, grp))
            for tp, grp in groupby(sorted(items, key=lambda x: type(x).__name__), type)
        }

    @property
    def updated(self) -> Dict[str, List[Dict]]:
        """
        Retrieve all updated entities along with their differences.

        Returns:
            Dict[str, List[Dict]]: A mapping from model name to list of diff dicts.
        """
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
    def deleted(self) -> Dict[str, List[int]]:
        """
        Retrieve all deleted entities grouped by model type.

        Returns:
            Dict[str, List[int]]: A mapping from model name to list of deleted IDs.
        """
        return {
            tp.__name__: [x.id for x in grp]
            for tp, grp in groupby(
                sorted(self.changes['deleted'], key=lambda x: type(x).__name__),
                type
            )
        }

    @property
    def m2m(self) -> List[Any]:
        """
        Placeholder for many-to-many relationship changes.
        Returns:
            List[Any]: Placeholder list (currently empty).
        """
        return []

    def register_session(self, session: Session):
        """
        Register a SQLAlchemy session for change tracking.

        Listens to flush and commit events on the given session to capture
        entity changes.

        Args:
            session (Session): The SQLAlchemy ORM session to monitor.
        """
        event.listen(session, 'before_commit', self._on_after_commit)
        event.listen(session, 'before_flush', self._on_before_flush)

    def _on_before_flush(self, session: Session, transaction: UOWTransaction, *args):
        """
        Callback triggered before a flush occurs. Tracks new, updated, and deleted
        entities in the session.

        Args:
            session (Session): The session being flushed.
            transaction (UOWTransaction): The unit of work transaction.
            *args: Additional arguments passed by the event system.
        """
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
        """
        Callback triggered after a commit. Currently prints a log message.

        Args:
            session (Session): The session that was committed.
        """
        print('--> send changes <--')

    def _on_before_commit(self, session):
        """
        Callback triggered before a commit. Stores tracked changes in the request context.

        Args:
            session (Session): The session about to be committed.
        """
        print('--> before commit <--')
        request.results = {
            'new': self.new,
            'updated': self.updated,
            'deleted': self.deleted,
            'm2m': self.m2m,
        }

    def _load_model(self, target: DeclarativeBase, context):
        """
        Callback triggered when a model is loaded from the database.
        Stores a serialized copy of the loaded object for later diffing.

        Args:
            target (DeclarativeBase): The model instance that was loaded.
            context: The load event context.
        """
        resource = self.rm.resources[target.__class__]
        request.loaded[target.__class__.__name__].append(resource.serialize(target))

    def _m2m_append(self, target, value, initiator):
        """
        Callback triggered when an item is added to a many-to-many relationship.

        Args:
            target: The source object of the M2M relation.
            value: The related object being added.
            initiator: The event initiator (used to get the property name).
        """
        self.changes['m2m'].append(('add', type(value).__name__, initiator.key, [value.id, target.id]))

    def _m2m_remove(self, target, value, initiator):
        """
        Callback triggered when an item is removed from a many-to-many relationship.

        Args:
            target: The source object of the M2M relation.
            value: The related object being removed.
            initiator: The event initiator (used to get the property name).
        """
        self.changes['m2m'].append(('del', type(value).__name__, initiator.key, [value.id, target.id]))

    def register_model(self, model):
        """
        Register all many-to-many relationships of a model to listen for changes.
        Also registers the load event listener to capture object footprints.
        Args:
            model: The SQLAlchemy declarative base model class to register.
        """
        event.listen(model, 'load', self._load_model)
        m2ms = (prop for prop in model.__mapper__.relationships if
                prop.direction == RelationshipDirection.MANYTOMANY)

        for m2m in m2ms:
            event.listen(m2m, 'append', self._m2m_append)
            event.listen(m2m, 'remove', self._m2m_remove)
