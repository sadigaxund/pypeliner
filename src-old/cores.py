
from . import types as TP

class AbstractCore(): ...

class LoaderCore(AbstractCore):
    #############
    # MAIN
    #############

    def __init__(self, *udf_loaders: TP.Function) -> TP.Void:
        self.__loaders = (no_change,) + udf_loaders

    def load(self, value: TP.Whatever) -> TP.Whatever:
        for loader in self.__loaders:
            loader(value)
            

class FunnelCore(AbstractCore):
    #############
    # MAIN
    #############

    def __init__(self, udf_merger: TP.Function) -> TP.Void:
        self.__merger = udf_merger

    def merge(self, *values: TP.Whatever) -> TP.Whatever:
        return self.__merger(*values)


class JunctionCore(AbstractCore):
    #############
    # MAIN
    #############

    def __init__(self, udf_divisor: TP.Function) -> TP.Void:
        self.__divisor = udf_divisor

    def divide(self, value) -> TP.Whatever:
        return self.__divisor(value)
