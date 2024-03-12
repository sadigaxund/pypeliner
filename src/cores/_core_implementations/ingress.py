# UTILS
from ._templates import void_available, void_iterate
from .abtract import AbstractCore
from enum import Enum

import itertools

# TYPING
from abc import (
    ABCMeta as AbstractMetadata,
    abstractmethod as AbstractMethod
)
from typing import (
    Iterable,
    Any,
    NoReturn,
)


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
                
class IngressCore(AbstractCore, metaclass=IngressMetaCore):
    Types = IngressType
    _type: IngressType = None
    
    def __init__(self, flatten = False, forgiving = False, input: Iterable | Any =None) -> NoReturn:
        self.flatten = flatten
        self.forgiving = forgiving
        self.input = input
        self.handler = IterationHandler()
        
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if cls._type == IngressType.INPUT:
            # KEEP 'input' & un-abstract others
            setattr(cls, 'available', void_available)
            setattr(cls, 'iterate', void_iterate)
        elif cls._type == IngressType.FUNCTION:
            # KEEP 'available' & 'iterate' & un-abstract others
            # setattr(cls, 'process_input', void_input_setter)
            ...
        else:
            raise AttributeError("When extending 'IngressCore' specify 'Type' as meta variable of type 'IngressType'")
    
    def __iter__(self):
        return self

    def __next__(self):
        # TODO: Probably inefficient but works, make it better 
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
    
    @AbstractMethod
    def available(self) -> bool: ...

    @AbstractMethod
    def iterate(self) -> NoReturn: ...
    
    @AbstractMethod
    def constructor(self): ...

    @AbstractMethod
    def destructor(self, exc_type, exc_value, traceback): ...
    
    @AbstractMethod
    def produce(self) -> Iterable | Any: ... 