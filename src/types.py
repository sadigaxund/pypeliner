from typing import (
    Any as Whatever,
    Callable as Function,
    Collection,
    Dict as Dictionary,
    Generator,
    Iterator,
    List,
    Tuple,
    Iterable, 
    NoReturn as Void,
    Optional as Maybe,
    Union as OneOf,
)
from abc import (
    ABC as AbstractClass, 
    ABCMeta as AbstractMetadata, 
    abstractmethod as AbstractMethod
)


NodeInputType = Iterable[Whatever]
NodeOutputType = Generator[Whatever, None, None]

InputNotLinkedError = NotImplementedError("Input was not linked!")
OutputNotLinkedError = NotImplementedError("Output was not linked!")


def NoInputAttributeWarning(cls):
    return UserWarning(f'{cls.__class__.__name__} does not take input.')

def NoOutputAttributeWarning(cls):
    return UserWarning(f'{cls.__class__.__name__} does not produce output.')
