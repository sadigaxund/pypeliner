from pydantic import BaseModel, AnyHttpUrl
from typing import Dict
from enum import Enum

class HTTPMethod(str, Enum):
    get     = 'GET'
    post    = 'POST'
    put     = 'PUT'
    delete  = 'DELETE'
    patch   = 'PATCH'

class Query(BaseModel):
    base_url: AnyHttpUrl
    http_method: HTTPMethod
    headers: Dict
    parameters: Dict
    body: Dict
