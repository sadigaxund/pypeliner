from ...types import *
from ...types.custom import * 
from ...cores import AbstractCore

class AbstractNode(AbstractClass, metaclass=AbstractMetadata):
    def __init__(self, core: AbstractCore) -> Void:
        self.core = core

    @property
    def input(self):
        return self.__input
    
    @input.setter
    def input(self, value):
        self.__input = value

    @property
    def output(self):
        return self.__output

    @output.setter
    def output(self, value):
        self.__output = value
        
    def __rshift__(self, other: 'AbstractNode'):
        other.input = self.output
        return other
    
    def __lshift__(self, other: 'AbstractNode'):
        self.input = other.output
        return self