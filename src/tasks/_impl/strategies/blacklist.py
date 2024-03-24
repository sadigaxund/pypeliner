from .._templates import void_exception_handler
from .base import BaseStrategy
from typing import Any, Callable, NoReturn

class BlacklistStrategy(BaseStrategy):
    def __init__(self, 
                 *exception_list,
                 fallback_value: Any = None, 
                 on_error: Callable[[type, str, type], Any] = void_exception_handler, 
                 ) -> NoReturn:
        super().__init__(fallback_value=fallback_value, 
                         on_error=on_error, 
                         blacklist=False, 
                         whitelist=True)