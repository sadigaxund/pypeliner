from typing import Dict, Union
from pathlib import Path
import json
import uuid

_ID_POOL_ = 0

class Task():
    
    def build(dict: Dict, id=str(uuid.uuid4()), type=None, source=None, metadata=None):
        return Task(
            ID=dict.get('id', id),
            Type=type or dict.get('type'),
            Source=source or dict.get('source'),
            Metadata=metadata or dict.get('metadata'),
        )
    
    def dissolve(self):
        return self.__dict__
    
    def __init__(self, Type: str, Source: str, Metadata: Dict = {}, ID: str = None):
        self.id = ID if ID else str(uuid.uuid4())
        self.type = Type
        self.source = Source
        self.metadata = Metadata
    
    def to_failed_task(self, extra: Union[str, Dict] = {}):
        if not isinstance(extra, dict):
            extra = {'message': extra}
        return FailedTask(self.type, self.source, {**self.metadata, **extra}, self.id)
    
    def setType(self, new_type) -> None:
        self.type = new_type

    def withType(self, new_type) -> 'Task':
        new_task = self.copy()
        new_task.setType(new_type)
        return new_task

    def copy(self) -> 'Task':
        return Task.build(self.dissolve().copy())
    
    def __eq__(self, other):
        if isinstance(other, Task):
            return self.id == other.id
        return False
    
    def __hash__(self):
        return hash(self.id)
    
    def __str__(self):
        from .functions import custom_encoder
        return json.dumps(self.dissolve(), default=custom_encoder)


class FailedTask(Task):
    ...


class Resource:
    _instance = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __new__(self, **kwargs):
        if self._instance is None:
            self._instance = super(Resource, self).__new__(self)
            # self._instance.KAFKA_PRODUCER = None
            # self._instance.Cache = None
            # self._instance.SNOWFLAKE_CURSOR = None
            # self._instance.SNOWFLAKE_CONNECTION = None
        return self._instance


class Config:
    _instance = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __new__(self, **kwargs):
        if self._instance is None:
            self._instance = super(Config, self).__new__(self)
            # self._instance.CACHE_PATH = Path('res/cache.db').resolve()
            # self._instance.SOURCE_CONF_PATH = Path('res/conf.toml').resolve()
            # self._instance.KAFKA_TOPIC = None
            # self._instance.EARLIEST_DATE = datetime(year=2000, month=1, day=1)
            # self._instance.DATE_FORMAT = None
            # self._instance.USERDEFINED_START_DATE = None
            # self._instance.USERDEFINED_END_DATE = None
            # self._instance.TIMESTAMP_FORMAT = '%Y-%m-%d'
        return self._instance



if __name__ == '__main__':
    task = Task("Hey", "Hello", {1: 2})
    print(task.to_failed_task().__dict__)
