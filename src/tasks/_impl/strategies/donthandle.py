from .._templates import void_exception_handler
from .base import BaseStrategy
from typing import Any, Callable, NoReturn

# Purely for readability
class DontHandleStrategy(BaseStrategy):
    def __init__(self, 
                 on_error: Callable[[type, str, type], Any] = void_exception_handler, 
                 exit_code: int = -1) -> NoReturn:
        super().__init__(on_error=on_error, 
                         blacklist=True, 
                         whitelist=False, 
                         exit_code=exit_code)