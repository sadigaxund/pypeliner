from ...types import *
from ...types.custom import * 
from ...cores import AbstractCore

class AbstractNode(AbstractClass, metaclass=AbstractMetadata):
    def __init__(self, core: AbstractCore) -> Void:
        self.core = core
        self.__input = NotImplementedError(
            "The Node implementation does not feature input capability.")
        self.__output = NotImplementedError(
            "The Node implementation does not feature output capability.")

    @AbstractMethod
    def run(): ...

    @property
    def input(self):
        if isinstance(self.__input, NotImplementedError):
            raise self.__input

        return self.__input
    
    @input.setter
    def input(self, value):
        self.__input = value

    @property
    def output(self):
        if isinstance(self.__output, NotImplementedError):
            raise self.__output

        return self.__output

    @output.setter
    def output(self, value):
        self.__output = value