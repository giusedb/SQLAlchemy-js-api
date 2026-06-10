# SQLAlchemy JS API (`jsalchemy-api`) — Documentation

**Package**: `sqlalchemy-js-api`
**Import**: `jsalchemy_api`
**Version**: 0.1.1
**Repository**: `github.com:giusedb/SQLAlchemy-js-api.git`
**Build**: Hatchling

## Overview

`jsalchemy-api` is the core routing and resource management layer of the JSAlchemy ecosystem. It provides the bridge between HTTP requests and SQLAlchemy ORM models, enabling automatic CRUD endpoint generation, resource registration, and API routing.

Think of it as an automatic REST API generator for SQLAlchemy models — but with full control via custom verbs and resource classes.

## Key Components

### ResourceManager

The central orchestrator. It:
- Manages authentication via an `AuthenticationManager` (from `jsalchemy-authentication`)
- Registers and routes requests to `WebResource`/`DBResource` instances
- Handles token-based session management
- Provides `action(token, resource, verb, **data)` as the universal entry point

### WebResource / DBResource

Base classes for defining API endpoints tied to database models:

| Class | Purpose |
|---|---|
| `WebResource` | Base for any web resource |
| `DBResource(WebResource)` | Pre-built CRUD for SQLAlchemy models |

A DBResource automatically registers these verbs:
- **list** — Paginated query with filtering, sorting
- **get** — Single record by primary key
- **post** — Create new record
- **put** — Update record
- **delete** — Remove record
- **bulk** — Batch insert
- **describe** — Model schema introspection

### Custom Verbs

Use the `@verb` decorator to add custom endpoints:

```python
@verb(detached_instance=True)
async def delete_all(self):
    await db.execute(delete(self.model).where(self.model.id > 0))
```

### Application Setup

`setup_application(config)` is the bootstrap function that:
1. Loads configuration (database, Redis, auth)
2. Creates a `ResourceManager` with the `AuthenticationManager`
3. Returns the fully configured `ResourceManager` ready for resource registration

### Response Format

All API responses follow a consistent JSON structure. The `describe` verb returns field metadata including types, defaults, required status, and primary keys.

## Features

- **Automatic CRUD**: Register a model as a `DBResource` and get full CRUD endpoints
- **Custom verbs**: Extend with `@verb` decorators for domain-specific operations
- **Pagination**: Configurable records-per-page (`rpp`) with cursor-based pagination
- **Model introspection**: Auto-discovers fields, types, and relationships
- **Token auth**: Built-in token extraction and validation
- **Async-first**: Full asyncio support via SQLAlchemy 2.0 async

## Dependencies

- `sqlalchemy>=2.0`
- `sqlalchemy-js-authentication` — Auth provider
- `sqlalchemy-js-authorization` — Permission checking
- `redis` / `fakeredis` — Session caching
- `orjson` — Fast JSON serialization
- `click` — CLI helpers

## Usage

```python
from jsalchemy_api import ResourceManager, DBResource
from jsalchemy_api.application import setup_application

config = get_config()
rm = setup_application(config)

class MyResource(DBResource):
    def __init__(self, rm):
        super().__init__(model=MyModel, name='MyModel', resource_manager=rm)

rm.register(MyResource(rm))

# All HTTP verbs now work automatically:
# POST /jsalchemy/MyModel.list
# POST /jsalchemy/MyModel.get   with {"id": 1, "__token__": "..."}
```