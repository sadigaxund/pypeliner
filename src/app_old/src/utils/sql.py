from sqlitedict import SqliteDict
from functools import partial
from pathlib import Path
from .logs import printlog
import json

# create new Exception
class CacheException(Exception):
    def __init__(self, message="Error while accessing cache", data=None):
        self.message = message
        if data:
            self.message += f": {data}"
        super().__init__(self.message)
class CacheWriteException(CacheException):
    def __init__(self, message="Error while writing data to cache", data=None):
        super().__init__(message, data)
class CacheReadException(CacheException):
    def __init__(self, message="Error while reading data from cache", data=None):
        super().__init__(message, data)

class DatabaseHandler():
    def __init__(self, file, table='default', flag='r', autocommit=True):
        if isinstance(file, str):
            file = Path(file)
        
        if flag != 'c' and not file.exists():
            raise FileNotFoundError(
                f"Cache file {file} does not exist. Please run the './templates/update-sqllite-from-json.py' to generate it.")
        
        
        self.db_file = file
        self.autocommit = autocommit
        self.table = table
        self.flag = flag
        self.table= table
        self.db = SqliteDict(self.db_file, 
                             tablename=self.table, 
                             flag=self.flag, 
                             journal_mode='OFF', 
                             autocommit=self.autocommit, 
                             encode=partial(json.dumps, ensure_ascii=False), 
                             decode=json.loads)
        # printlog("Connected to Database", 'debug', self.json())
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.clean()
    
    def json(self):
        return {
            "file" : str(self.db_file),
            "table" : self.table,
            "keys" : self.keys(),
            "flag" : self.flag,
        }
    
    def keys(self):
        try:
            return list(self.db.keys())
        except Exception as ex:
            raise CacheReadException(data={"file": self.db_file, "table" : self.table}) from ex

    def write(self, key, value):
        try:
            if self.flag == 'r':
                raise CacheWriteException("Cannot write with flag='r'")
            self.db[key] = value
        except Exception as ex:
            raise CacheWriteException(data={"table" : self.table, "key": key, "value": value}) from ex 
    
    def read(self, key):
        try:
            return self.db[key]
        except Exception as ex:
            raise CacheReadException(data={"table" : self.table, "key": key}) from ex 
    
    def read_safe(self, key):
        return self.db.get(key, None)
    
    def commit(self):
        self.db.commit()

    def close(self):
        self.db.close()
    
    def clean(self):
        self.commit()
        self.close()        