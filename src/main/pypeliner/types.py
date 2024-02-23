from typing import Generator, Any, Union, Iterator, Collection, Callable, Optional, List, Dict

DefaultInputType = Union[Generator, Iterator, Collection]
DefaultOutputType = Generator[Any, None, None]

InputNotLinkedError = NotImplementedError("Input was not linked!")
OutputNotLinkedError = NotImplementedError("Output was not linked!")


def NoInputAttributeWarning(cls):
    return UserWarning(f'{cls.__class__.__name__} does not take input.')


def NoOutputAttributeWarning(cls):
    return UserWarning(f'{cls.__class__.__name__} does not produce output.')
