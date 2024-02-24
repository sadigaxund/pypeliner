
from ....main.pypiliner.types import *
from typing import NoReturn, Callable
# Seed has behavior:
# * available        - tells if there might be next phase
# * iterate     - returns new seed with updated parameters
# * grow        - executes based on given parameters & saves result

# Steps:
# * first time: runs, returns result, sets state
# * every other: checks state, if available then run again, reset state
# # if no more: then return stop iteration


class ProcessNode:
    #############
    # MAIN
    #############

    def __init__(self, udf_execute=None, udf_iterate=None, udf_available=None) -> NoReturn:
        self.__any_available = True
        self.execute = udf_execute if udf_execute else self.__execute__
        self.iterate = udf_iterate if udf_iterate else self.__iterate__
        self.available = udf_available if udf_available else self.__available__

    def __iter__(self):
        return self

    def __next__(self):
        if not self.__any_available:
            raise StopIteration

        result = self.execute()
        if self.available:
            self.iterate()

        return result

    #############
    # INNER
    #############

    def __execute__(self):
        raise NotImplementedError(
            "Implement 'execute' behavior to run the node!")

    def __available__(self):
        raise NotImplementedError(
            "Implement 'available' behavior to run the node!")

    def __iterate__(self):
        raise NotImplementedError(
            "Implement 'iterate' behavior to run the node!")

    #############
    # PUBLIC
    #############

    @property
    def execute(self) -> Callable:
        return self.__execute__

    @execute.setter
    def execute(self, new_behaviour: Callable) -> NoReturn:
        self.__execute__ = new_behaviour

    @property
    def available(self) -> bool:
        self.__any_available = self.__available__()
        return self.__any_available

    @available.setter
    def available(self, new_behaviour: Callable) -> NoReturn:
        self.__available__ = new_behaviour

    @property
    def iterate(self) -> Callable:
        return self.__iterate__

    @iterate.setter
    def iterate(self, new_behaviour: Callable) -> NoReturn:
        self.__iterate__ = new_behaviour
