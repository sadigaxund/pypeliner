from .abtract import AbstractCore
from ...types.custom import *
from ...types import *
from ._templates import void_available, void_execute, void_input_setter, void_iterate
import itertools

class IngressType(Enum):
    # Input based Ingress: user sets a seed in form of a iterable
    # Process finishes when the given input gets exhausted
    # Kind of deterministic approach where you know when the process is going to be over.
    INPUT = 0x10

    # Function based Ingress: user defines certain function that will be executed in certain order.
    # Process finishes when flag function returns False
    # Kind of probabilitic or indefinite approach, where the process completely depends on implementation of function (maybe external api's behavior or kafka's consuming)
    FUNCTION = 0x20


class IngressMetaCore(AbstractMetadata):
    def __new__(cls, name, bases, dct, Type: IngressType = None):
        if Type is not None and not isinstance(Type, IngressType):
            raise TypeError("Type parameter can only be of type IngressType.")
        dct['_type'] = Type
        return super().__new__(cls, name, bases, dct)


class IngressCore(AbstractCore, metaclass=IngressMetaCore):
    class IterationHandler:
        def __init__(self) -> None:
            self._iterator = itertools.chain()
            pass
        
        def append(self, value, flatten=False):
            if flatten:
                if isinstance(value, Iterable):
                    self._iterator = itertools.chain(value)
                    return
            self._iterator = itertools.chain([value])
                    
        
        def __iter__(self):
            return self

        def __next__(self):
            try:
                return next(self._iterator)
            except RuntimeError as generr:
                if str(generr) == 'generator raised StopIteration':
                    raise StopIteration
                else:
                    raise generr
    
    Types = IngressType
    _type: IngressType = None
    
    def __init__(self, flatten = False, forgiving = False, input=None) -> None:
        self.flatten = flatten
        self.forgiving = forgiving
        self.process_input(input)
        self.handler = IngressCore.IterationHandler()
        
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if cls._type == IngressType.INPUT:
            # KEEP 'input' & un-abstract others
            setattr(cls, 'available', void_available)
            setattr(cls, 'iterate', void_iterate)
        elif cls._type == IngressType.FUNCTION:
            # KEEP 'available' & 'iterate' & un-abstract others
            setattr(cls, 'process_input', void_input_setter)
        else:
            raise AttributeError("When extending 'IngressCore' specify 'Type' as meta variable of type 'IngressType'")
    
    def __iter__(self):
        return self

    def __next__(self):
        try:
            val = next(self.handler)
            return val
        except StopIteration:
            if self.available():
                self.iterate()
                self.handler.append(self.produce(), flatten=self.flatten)
                return next(self.handler)
        raise StopIteration

    def __enter__(self):
        self.constructor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.destructor(exc_type, exc_value, traceback)
    
    @property
    def input(self):
        return self._input
    
    @input.setter
    def input(self, new_input):
        if isinstance(new_input, Iterable):
            self._input = itertools.chain(new_input)
        else:
            self._input = itertools.chain([new_input])
        
    @AbstractMethod
    def process_input(self, input: Whatever) -> Iterable | Whatever: ...
    
    @AbstractMethod
    def available(self) -> Bool: ...

    @AbstractMethod
    def iterate(self) -> Void: ...
    
    @AbstractMethod
    def constructor(self): ...

    @AbstractMethod
    def destructor(self, exc_type, exc_value, traceback): ...
    
    @AbstractMethod
    def produce(self) -> Whatever: ... 