from typing import Optional, List, Annotated

from pydantic import BaseModel, Field


class Permission(BaseModel):
    pass

class Validator(BaseModel):
    pass

class Reference(BaseModel):
    resource: str
    description: Optional[str]
    type: Annotated[str, Field(strict=True)]
    attribute: str

class Field(BaseModel):
    name: str
    type: str
    constraints: Optional[List[str]]
    widget: Optional[str]
    validators: List[Validator]

class ModelDescription(BaseModel):
    name: str
    description: Optional[str]
    permissions: Optional[List[Permission]]
    fields: List[Field]
    UID: List[str]
    references: List[Reference]
