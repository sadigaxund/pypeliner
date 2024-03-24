# TOOLS
from ...cores import AbstractCore

# TYPING
from abc import (
    ABC as AbstractClass,
    ABCMeta as AbstractMetadata,
)
from typing import (
    Iterable,
    Any,
    NoReturn,
)
# Iterable | Any

class Shiftable:
    def __rshift__(self, other: 'AbstractNode'):
        other.input = self.output
        return other
    
    def __lshift__(self, other: 'AbstractNode'):
        self.input = other.output
        return self

class AbstractNode(AbstractClass, Shiftable, metaclass=AbstractMetadata):
    def __init__(self, core: AbstractCore) -> NoReturn:
        self.core = core
        self.__input=None
        self.__output=None

    @property
    def input(self) -> Iterable:
        return self.__input
    
    @input.setter
    def input(self, value: Iterable | Any):
        self.__input = value

    @property
    def output(self) -> Iterable:
        return self.__output

    # @output.setter
    # def output(self, value):
    #     self.__output = value
        
   