from .base import BasePolicy
from .._functools import clean_raise

from typing import Any, NoReturn, Iterable
from collections import deque
import time

class TimeBasedPolicy(BasePolicy):
    
    def __init__(self, intervals: Iterable) -> NoReturn:
        if isinstance(intervals, int):
            intervals = [intervals]
        
        self.intervals = deque(intervals)
        super().__init__()
    
    def execute(self, *args, **kwargs) -> Any:
        try:    
            return super().execute(*args, **kwargs)
        except Exception as e:
            if len(self.intervals) > 0:
                time.sleep(self.intervals.popleft())
                return self.execute(*args, **kwargs)
            else:
                clean_raise(e)
    
    class Linear:
        def __init__(self, start, stop, step=1) -> NoReturn:
            pass
    
    class Exponential:
        def __init__(self, base, n) -> NoReturn:
            pass
            
class Linear(TimeBasedPolicy):
    def __init__(self, start, stop, step=1) -> NoReturn:
        super().__init__(intervals=range(start, stop, step))          
            
class Exponential(TimeBasedPolicy):
    def __init__(self, base, n) -> NoReturn:
        print([base ** i for i in range(n)])
        super().__init__(intervals=[base ** i for i in range(n)])


TimeBasedPolicy.Linear = Linear
TimeBasedPolicy.Exponential = Exponential