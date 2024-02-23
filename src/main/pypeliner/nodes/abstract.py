from typing import NoReturn, Never
from abc import ABC, ABCMeta, abstractmethod
from ..cores.abstract import AbstractCore


class AbstractNode(ABC, metaclass=ABCMeta):
    def __init__(self, core: AbstractCore) -> NoReturn:
        self.core = core
        self.__input = NotImplementedError("The Node implementation does not feature input capability.")
        self.__output = NotImplementedError("The Node implementation does not feature output capability.")
    
    @abstractmethod
    def run(): ...
    
    @property
    def input(self):
        if isinstance(self.__input, NotImplementedError):
            raise self.__input

        return self.__input

    @property
    def output(self):
        if isinstance(self.__output, NotImplementedError):
            raise self.__output

        return self.__output

    @input.setter
    def input(self, value):
        self.__input = value

    @output.setter
    def output(self, value):
        self.__output = value
