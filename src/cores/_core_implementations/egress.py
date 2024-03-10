from .abtract import AbstractCore
from ...types.custom import *
from ...types import *
import itertools

class EgressCore(AbstractCore):
    
    def __init__(self, input, heartbeat=1) -> None:
        self.input = input
        self.heartbeat=heartbeat

    def __enter__(self):
        self.constructor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.destructor(exc_type, exc_value, traceback)
        
    def run(self):
        for i, record in enumerate(self.input, start=1):
            
            any_exception = None
            try:
                self.consume(record=record)
            except Exception as e:
                any_exception = e
            finally:
                # If callback recieves None as parameter
                self.callback(record = record,
                              exception=any_exception)
                
                if i % self.heartbeat == 0:
                    self.pulse()
    @property
    def input(self):
        return self._input
    
    @input.setter
    def input(self, new_input):
        if isinstance(new_input, Iterable):
            self._input = itertools.chain(new_input)
        else:
            self._input = itertools.chain([new_input])
        
    @AbstractMethod
    def constructor(self): ...
    @AbstractMethod
    def destructor(self, exc_type, exc_value, traceback): ...
    @AbstractMethod
    def callback(self, record, exception: Exception): ...
    @AbstractMethod
    def pulse(self): ...
    @AbstractMethod
    def consume(self, record: Whatever) -> Whatever: ... 