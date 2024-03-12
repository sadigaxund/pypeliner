from collections import deque
from typing import Union, List, Any, Callable
# from ..structs import Function
from .error import ErrorStrategy
from abc import ABC, ABCMeta, abstractmethod
import time

Function = Callable

class RetryPolicy:
    def __init__(self, 
                 retries = 3,
                 backoff_seconds = 3) -> None:
        self.retries = retries
        self.backoff_seconds = backoff_seconds
    
    @abstractmethod
    def attempt(self, 
                callback: Function,
                error_strategy: ErrorStrategy = ErrorStrategy.NeverHandle()): ...
    
    class AbstractPolicy:
        def attempt(self): ...
        
    class Exponential(AbstractPolicy): ...
    class Linear(AbstractPolicy): ...
    class Idempotent(AbstractPolicy): ...
    class Selective(AbstractPolicy): ...
    class RetryError(Exception): ...


class Selective(RetryPolicy):
    def __init__(self, intervals=[]) -> None:
        self.intervals = deque(intervals)
        self.retries = 0

    def attempt(self,
                callback: Function,
                error_strategy: ErrorStrategy = ErrorStrategy.NeverHandle()):
        try:
            self.retries += 1
            retval = error_strategy.execute(callback)
            # Case when code fails, even though error_strategy executed fine.
            if error_strategy.success:
                return retval
            else:
                raise RetryPolicy.RetryError(
                    f"Code failed at Attempt#{self.retries}")
        except:
            if len(self.intervals) == 0:
                raise RetryPolicy.RetryError(f"Could not execute the function within {self.retries} attempts")
            else:
                time.sleep(self.intervals.popleft())
                return self.attempt(callback, error_strategy)

class Exponential(Selective):
    def __init__(self, 
                 retries=3, 
                 backoff_seconds=3,
                 exp_factor = 1) -> None:
        intervals = []
        for i in range(retries):
            intervals.append(backoff_seconds ** exp_factor)
            exp_factor += 1
        super().__init__(intervals)


class Linear(Selective):
    def __init__(self,
                 retries=3,
                 backoff_seconds=3) -> None:
        intervals = [backoff_seconds for _ in range(retries)]
        super().__init__(intervals)

class Idempotent(RetryPolicy):
    def __init__(self):
        ...
        # super().__init__(backoff_seconds=)

    def attempt(self,
                callback: Function,
                error_strategy: ErrorStrategy = ErrorStrategy.NeverHandle()):
        print(error_strategy, callback)
        return error_strategy.execute(callback)


class TimeTriggered(RetryPolicy):
    # TODO: Some cronjob stuff here
    ...


class EventTriggered(RetryPolicy):
    # TODO: Idea: pass a function that will return either True or False
    # When False Retry, When True finish
    # Possible parameters: 
    #   * heartbeat (an interval of time between each check)
    #   * max_attempts
    #   * timeout
    ...


