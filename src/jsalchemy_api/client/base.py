from typing import Dict, List, Optional, Any, Callable

from pydantic import BaseModel, ConfigDict


class LoginInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    username: str
    password: str


class Config(BaseModel):
    model_config = ConfigDict(frozen=True)

    base_url: str
    autologin: bool
    keep_session: int
    ws: Optional[dict]
    login: LoginInfo
    resolution_period: int=500


class Resource:
    _data: Dict[str, Any]
    get_pk: Callable[[Dict[str, Any]], str]
    UPDATE: Callable[[Dict[str, Any]], None]
    PK: str=None
    RPP: int
    DIRTY: bool
    DIFF: Dict[str, Any]
    GET: Callable[[List[str], bool ], 'Resource']
    RAW: Dict[str, Any]
    COLLECTION: 'Collection'
    create: Callable[[Dict[str, Any]], 'Resource']


class VerbDef(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    isInstance: bool
    args: List[str]
    defaults: Dict[str, Any]
    detachReturn: bool


class FieldDef(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    description: Optional[str]
    type: str
    validators: List
    readonly: bool
    extra: Dict | None = None


class ReferenceDef(BaseModel):
    model_config = ConfigDict(frozen=True)

    resource: str
    type: str
    attribute: str
    foreign_attribute: str
    description: str | None
    local_attribute: str
    is_pk: bool | None = False


class ResourceDefinition(BaseModel):
    model_config = ConfigDict(frozen=True)

    PK: List[str]
    fields: List[FieldDef]
    references: List[ReferenceDef]
    format_string: str
    verbs: List[VerbDef]
    rpp: int
    description: str | None = ''
    name: str
