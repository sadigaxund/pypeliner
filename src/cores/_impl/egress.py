# UTILS
from .abtract import AbstractCore
import itertools
import time

# TYPING
from abc import abstractmethod as AbstractMethod

from typing import (
    Iterable,
    Any,
    NoReturn,
)


class EgressCore(AbstractCore):
    
    def __init__(self, input:Iterable | Any = None, heartbeat:int = 1, forgiving=False, backpressure:int = 0) -> NoReturn:
        self.input = input
        self.heartbeat = heartbeat
        self.forgiving = forgiving
        self.backpressure = backpressure

    def __enter__(self):
        self.constructor()
        return self
        

    def __exit__(self, exc_type, exc_value, traceback):
        self.destructor(exc_type, exc_value, traceback)
        
    def run(self) -> NoReturn:
        for i, record in enumerate(self.input, start=1):
            any_exception = None
            try:
                self.consume(record)
            except Exception as e:
                any_exception = e
            finally:
                # If callback recieves None as parameter
                self.callback(record, any_exception)
                
                if self.forgiving == False and any_exception is not None:
                    raise e 
                
                if i % self.heartbeat == 0:
                    self.pulse()
                    time.sleep(self.backpressure)
        
        
        # Run pulse function one last time 
        # for the remaining records where i % self.heartbeat > 0
        if i % self.heartbeat > 0:
            self.pulse()
                
    @property
    def input(self) -> Iterable:
        return self._input
    
    @input.setter
    def input(self, new_input):
        if not isinstance(new_input, Iterable):
            new_input = itertools.chain([new_input])
        
        self._input = new_input
        
    @AbstractMethod
    def constructor(self) -> NoReturn: ...
    @AbstractMethod
    def destructor(self, exc_type, exc_value, traceback) -> NoReturn: ...
    @AbstractMethod
    def callback(self, record: Any, exception: Exception) -> NoReturn: ...
    @AbstractMethod
    def pulse(self) -> NoReturn: ...
    @AbstractMethod
    def consume(self, record: Any) -> NoReturn: ... 