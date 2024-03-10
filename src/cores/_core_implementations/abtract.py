from ...types import *
from ...types.custom import * 
import itertools

class AbstractCore(AbstractClass):
    @property
    def input(self):
        return self._input
    
    @input.setter
    def input(self, new_input):
        if isinstance(new_input, Iterable):
            self._input = itertools.chain(new_input)
        else:
            self._input = itertools.chain([new_input])