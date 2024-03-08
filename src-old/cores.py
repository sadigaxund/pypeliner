
from . import types as TP

__all__ = [
    'AbstractCore',
    'ExtractorCore',
    'LoaderCore',
    'ProcessorCore',
    'JunctionCore',
    'FunnelCore',
]

no_change = lambda x: x

class AbstractCore(): ...

class ExtractorCore(AbstractCore):
    #############
    # MAIN
    #############

    def __init__(self, udf_execute=None, udf_iterate=None, udf_available=None, udf_input_setter=None, input=None) -> TP.Void:
        self.any_available=True
        self.execute = udf_execute if udf_execute else self.__execute__
        self.iterate = udf_iterate if udf_iterate else self.__iterate__
        self.available = udf_available if udf_available else self.__available__
        self.input_setter = udf_input_setter
        self.input = input
        

    def __iter__(self):
        return self

    def __next__(self):
        if not self.any_available:
            raise StopIteration
        
        result = self.execute()
        
        if self.available:
            self.iterate()
            
        return result
    
    # #############
    # # INNER
    # #############

    def __execute__(self):
        raise NotImplementedError(
            "Implement 'execute' behavior to run the node!")

    def __available__(self):
        raise NotImplementedError(
            "Implement 'available' behavior to run the node!")

    def __iterate__(self):
        raise NotImplementedError(
            "Implement 'iterate' behavior to run the node!")
    
    
    # #############
    # # PUBLIC
    # #############
    
    @property
    def input(self) -> TP.Function:
        return self.__execute__

    @input.setter
    def input(self, new_input: TP.Function) -> TP.Void:
        if self.input_setter and new_input:
            self.input_setter(new_input)
            

    @property
    def execute(self) -> TP.Function:
        return self.__execute__

    @execute.setter
    def execute(self, new_behaviour: TP.Function) -> TP.Void:
        self.__execute__ = new_behaviour

    @property
    def available(self) -> bool:
        self.any_available = self.__available__()
        return self.any_available

    @available.setter
    def available(self, new_behaviour: TP.Function) -> TP.Void:
        self.__available__ = new_behaviour

    @property
    def iterate(self) -> TP.Function:
        return self.__iterate__

    @iterate.setter
    def iterate(self, new_behaviour: TP.Function) -> TP.Void:
        self.__iterate__ = new_behaviour

class ProcessorCore(AbstractCore):
    #############
    # MAIN
    #############

    def __init__(self,
                 *udf_transformers: TP.Function,
                 immutable=False,    # decides if process affect the value or not
                 raise_error=False,
                 forgiving=False,   # decides if process should be forgiving the error
                 fallback=None,   # this value will returned if process is not forgiving
                 ) -> TP.Void:
        self.__processors = (no_change,) + udf_transformers
        from copy import deepcopy
        self.clone = deepcopy if immutable else no_change
        self.raise_error = raise_error
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
                    if self.raise_error:
                        raise e
                    else:
                        return self.fallback
        return retval

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
