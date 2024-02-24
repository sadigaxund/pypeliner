
from pypeliner import types as TP

__all__ = [
    'AbstractCore',
    'ExtractorCore',
    'LoaderCore',
    'ProcessorCore',
]

no_change = lambda x: x

class AbstractCore(): ...

class ExtractorCore(AbstractCore):
    #############
    # MAIN
    #############

    def __init__(self, udf_execute=None, udf_iterate=None, udf_available=None) -> TP.Return0:
        self.any_available = True
        self.execute = udf_execute if udf_execute else self.__execute__
        self.iterate = udf_iterate if udf_iterate else self.__iterate__
        self.available = udf_available if udf_available else self.__available__

    def __iter__(self):
        return self

    def __next__(self):
        if not self.any_available:
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
    def execute(self) -> TP.Function:
        return self.__execute__

    @execute.setter
    def execute(self, new_behaviour: TP.Function) -> TP.Return0:
        self.__execute__ = new_behaviour

    @property
    def available(self) -> bool:
        self.any_available = self.__available__()
        return self.any_available

    @available.setter
    def available(self, new_behaviour: TP.Function) -> TP.Return0:
        self.__available__ = new_behaviour

    @property
    def iterate(self) -> TP.Function:
        return self.__iterate__

    @iterate.setter
    def iterate(self, new_behaviour: TP.Function) -> TP.Return0:
        self.__iterate__ = new_behaviour

class ProcessorCore(AbstractCore):
    #############
    # MAIN
    #############

    def __init__(self,
                 *udf_transformers: TP.Function,
                 immutable=False,    # decides if process affect the value or not
                 forgiving=False,   # decides if process should be forgiving the error
                 fallback=None,   # this value will returned if process is not forgiving
                 ) -> TP.Return0:
        self.__processors = (no_change,) + udf_transformers
        from copy import deepcopy
        self.clone = deepcopy if immutable else no_change
        self.forgiving = forgiving
        self.fallback = fallback

    def process(self, value: TP.Whatever) -> TP.Whatever:
        # Copy or not the value
        retval = self.clone(value)
        for processor in self.__processors:
            # Apply every processor on the value
            try:
                retval = processor(retval)
            except Exception as e:
                if self.forgiving:
                    continue
                else:
                    return self.fallback
        return retval

class LoaderCore(AbstractCore):
    #############
    # MAIN
    #############

    def __init__(self, *udf_loaders: TP.Function) -> TP.Return0:
        self.__loaders = (no_change,) + udf_loaders

    def load(self, value: TP.Whatever) -> TP.Whatever:
        for loader in self.__loaders:
            loader(value)