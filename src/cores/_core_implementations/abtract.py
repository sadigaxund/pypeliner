from ...types import *
from ...types.custom import * 
import itertools

class AbstractCore(AbstractClass):
    @property
    def input(self):
        return self._input
    
    @input.setter
    def input(self, new_input):
        if not isinstance(new_input, Iterable):
            new_input = [new_input]
            
        self._input = itertools.chain(new_input)