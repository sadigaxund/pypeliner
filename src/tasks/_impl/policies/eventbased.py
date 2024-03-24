from typing import Any, NoReturn, Callable
from .base import BasePolicy
from .._templates import DoesNothing
from .._functools import clean_raise
import time

class EventBasedPolicy(BasePolicy):
    
    def __init__(self, heartbeat:int = 0, attempts:int = -1, event:Callable = DoesNothing, ) -> NoReturn:
        self.attempts = attempts
        self.heartbeat = heartbeat
        self.event = event
        super().__init__()
    
    def execute(self, *args, **kwargs) -> Any:
        try:    
            self.attempts -= 1
            return super().execute(*args, **kwargs)
        except Exception as e:
            if self.attempts == 0:
                clean_raise(e)
            
            if not self.event():
                time.sleep(self.heartbeat)
                return self.execute(*args, **kwargs)
            