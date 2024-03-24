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

class BaseStrategy(Procedure, AbstractClass):
    
    def __init__(self, 
                 fallback_value: Any = None,
                 on_error: Callable[[type, str, type], Any] = void_exception_handler,
                 blacklist: Union[List, bool] = False,
                 whitelist: Union[List, bool] = False, # means handle all
                 exit_code: int = -1
                 ) -> NoReturn:
        
        
        self.fallback_value = fallback_value
        self.on_error = on_error
        self.blacklist = blacklist
        self.whitelist = whitelist
        self.exit_code = int(exit_code)
        self.success = True
        self.__post_init__()
        
        self.decorated = False
        
    def __call__(self, *args, **kwargs) -> Any:
        if not self.decorated:
            self.decorated = True
            super().__init__(args[0])
            return self
        else:
            return self.execute(*args, **kwargs)
        
        
    # Some Validation Rules for Error Handling Strategies:
    def __post_init__(self):
        if self.blacklist & self.whitelist:
            raise ValueError("Can not set both whitelist and blacklist at the same time.")
        if self.exit_code < -1 or self.exit_code > 127:
            raise ValueError("Bad exit code. Code should be 0 for successful termination or 1-127 for exiting with a specific error.")
        
    
    def __check_if_anylisted__(self, exception, anylist):
        if anylist:
            if anylist == True:
                return True
            else:
                return exception in anylist
        return False
    
    def __check_if_blacklisted__(self, exception):
        return self.__check_if_anylisted__(exception, self.blacklist)
            
    def __check_if_whitelisted__(self, exception):
        return self.__check_if_anylisted__(exception, self.whitelist)
    
    def execute(self, *args, **kwargs) -> Any:
        try:
            return super().execute(*args, **kwargs)
        except Exception as exc:
            blacklisted = self.__check_if_blacklisted__(exc)
            whitelisted = self.__check_if_whitelisted__(exc)
            self.on_error(type(exc), str(exc), exc.__traceback__)
            
            # If the occured exception was blacklisted
            if blacklisted:
                # If user selected to end the program with code
                if self.exit_code != -1:
                    import sys
                    sys.exit(self.exit_code)
                else:
                    exc.__traceback__ = None
                    raise
            
            if whitelisted:
                return self.fallback_value
            
            return exc
