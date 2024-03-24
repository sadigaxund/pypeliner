# UTILS
from .._templates import void_exception_handler
from ..procedure import Procedure

# TYPING
from abc import (
    ABC as AbstractClass,
    ABCMeta as AbstractMeta,
    abstractmethod as AbstractMethod
)

from typing import Callable, List, Union, Any, NoReturn, Iterable

class BasePolicy(Procedure, AbstractClass):
    
    def __init__(self) -> NoReturn:
        self.decorated = False
        
    def __call__(self, *args, **kwargs) -> Any:
        if not self.decorated:
            self.decorated = True
            super().__init__(args[0])
            return self
        else:
            return self.execute(*args, **kwargs)
        
    @AbstractMethod
    def execute(self, *args, **kwargs) -> Any:
        return super().execute(*args, **kwargs)
        