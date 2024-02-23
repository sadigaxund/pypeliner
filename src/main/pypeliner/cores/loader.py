
from ..types import *
from .abstract import AbstractCore
from typing import NoReturn, Callable



no_change = lambda x: x

class LoaderCore(AbstractCore):
    #############
    # MAIN
    #############

    def __init__(self,
                 *udf_loaders: Callable,
                 ) -> NoReturn:
        self.__loaders = (no_change,) + udf_loaders

    def load(self, value: Any) -> Any:
        for loader in self.__loaders:
            loader(value)
