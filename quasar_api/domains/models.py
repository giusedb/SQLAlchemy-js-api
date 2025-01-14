from typing import Literal, List, Optional

from pydantic import BaseModel


class Validator(BaseModel):
    name: str
    args: List


class ResourceField(BaseModel):
    name: str
    description: str
    type: Literal['string', 'integer', 'float', 'boolean', 'date', 'datetime', 'list']
    validators: Optional[List[Validator]]
    widget: str

class Description(BaseModel):
    name: str
    description: str
    fields: List[ResourceField]
    pk: List[str]

class Relation(BaseModel):
    to: str
    type: Literal['one', 'multiple']
    


if __name__ == '__main__':
    desc = Description(
        name='testa',
        description='test',
        fields=[
            ResourceField(name='name',
                          description='test',
                          type='string',
                          widget='rt-editor',
                          validators=[
                              Validator(name='regex', args=[r'\w+@\w+\.\w+'])
                          ]),
        ],
        pk=['test', 'test2']
    )
