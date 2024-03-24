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
    Generator
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
    class Defaults(Enum):
        NULL_VALUE = 0x123
        STOP_VALUE = 0x234
    
    def __init__(self) -> None:
        self.iterators = itertools.chain([])
        
    def append(self, genexpr, flatten = False):
        if isinstance(genexpr, Generator):
            self.iterators = itertools.chain(self.iterators, genexpr)
        else:
            if flatten:
                self.iterators = itertools.chain(self.iterators, genexpr)
            else:
                self.iterators = itertools.chain(self.iterators, [genexpr])
    
    def __iter__(self):
        return self

    def __next__(self):
        return next(self.iterators)
                    
class IngressCore(AbstractCore, metaclass=IngressMetaCore):
    Types = IngressType
    _type: IngressType = None
    
    def __init__(self, flatten = False, input: Iterable | Any = None) -> NoReturn:
        self.flatten = flatten
        self.input = input
        self.handler = IterationHandler()
        
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if cls._type == IngressType.INPUT:
            setattr(cls, 'available', void_available)
            setattr(cls, 'iterate', void_iterate)
        elif cls._type == IngressType.FUNCTION:
            ...
        else:
            raise AttributeError("When extending 'IngressCore' specify 'Type' as meta variable of type 'IngressType'")
    
    def __iter__(self):
        return self

    def __next__(self):

        while True:
            try:
                # Exhaust Input Handler first
                return next(self.handler)
            except StopIteration:
                # If handler exhausted then try to load
                self.handler.append(self.produce(), self.flatten)

                if self.available():
                    self.iterate()
                else:
                    # If no more available stop all
                    raise StopIteration
            except RuntimeError as e:
                if str(e) == 'generator raised StopIteration':
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
    
    # @AbstractMethod
    def constructor(self): ...

    # @AbstractMethod
    def destructor(self, exc_type, exc_value, traceback): ...
    
    @AbstractMethod
    def produce(self) -> Iterable | Any: ... 