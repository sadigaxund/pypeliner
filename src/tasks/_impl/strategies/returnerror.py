from .._templates import void_exception_handler
from .base import BaseStrategy
from typing import Any, Callable, NoReturn

class ReturnErrorStrategy(BaseStrategy):
    def __init__(self, 
                 on_error: Callable[[type, str, type], Any] = void_exception_handler
                 ) -> NoReturn:
        super().__init__(on_error=on_error, 
                         blacklist=False, 
                         whitelist=False)