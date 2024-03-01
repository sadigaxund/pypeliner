from typing import Union, List, Any
from ..structs import Function
from .error import ErrorStrategy
from abc import ABC, ABCMeta, abstractmethod
import time

class RetryError(Exception):
    pass


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
    

class Exponential(RetryPolicy):
    def __init__(self, 
                 retries=3, 
                 backoff_seconds=3,
                 exp_factor = 1) -> None:
        super().__init__(retries, backoff_seconds)
        self.__retries = self.retries
        self.exp_factor = exp_factor

    def attempt(self,
                callback: Function,
                error_strategy: ErrorStrategy = ErrorStrategy.NeverHandle()):
        try:
            return error_strategy.execute(callback=callback)
        except Exception as e:
            self.retries -= 1
            if self.retries <= 0:
                raise RetryError(
                    f"Could not execute the function within {self.__retries} attempts")
            else:
                time.sleep(self.backoff_seconds ** self.exp_factor)
                self.backoff_seconds **= self.exp_factor
                return self.attempt(callback, error_strategy)


class Linear(RetryPolicy):
    def __init__(self,
                 retries=3,
                 backoff_seconds=3) -> None:
        super().__init__(retries, backoff_seconds)
        self.__retries = self.retries

    def attempt(self,
                callback: Function,
                error_strategy: ErrorStrategy = ErrorStrategy.NeverHandle()):
        try:
            val = error_strategy.execute(callback)
            return val
        except Exception as e:
            self.retries -= 1
            if self.retries <= 0:
                raise RetryError(
                    f"Could not execute the function within {self.__retries} attempts")
            else:
                time.sleep(self.backoff_seconds)
                return self.attempt(callback, error_strategy)


class Idempotent(RetryPolicy):
    def __init__(self):
        super().__init__()

    def attempt(self,
                callback: Function,
                error_strategy: ErrorStrategy = ErrorStrategy.NeverHandle()):
        return error_strategy.execute(callback)


class TimeTriggered(RetryPolicy):
    ...


class EventTriggered(RetryPolicy):
    ...


class Selective(RetryPolicy):
    ...
