

from typing import Union, List, Any
from ..structs import Function
from abc import ABC, ABCMeta, abstractmethod

def default_on_error(exception):
    # That does nothing
    ...

class ErrorStrategy(ABC, metaclass=ABCMeta):
    # Case 1::  No Handle Policy    ::  if nothing passed then no handle, only for readability
    # Case 2::  Handle & Drop       ::  just handle and forget, not recommended
    # Case 3::  Handle & Return     ::  handle error and return it
    # Case 4::  Handle & Return     ::  handle error and return fallback value
    # Case 5::  Select Errors       ::  select which error to handle, or otherwise select which ones to ignore
    # Case 6::  Set Exit Code       ::  if set, code will return with the code when exception is raise
    
    def __init__(self,
                 fallback_value: Any = None,
                 on_error: Function = default_on_error,
                 blacklist: Union[List, bool] = True,
                 whitelist: Union[List, bool] = False,
                 exit_code: int = -1,
                 ) -> None:
        self.fallback_value = fallback_value
        self.on_error = on_error
        self.blacklist = blacklist
        self.whitelist = whitelist
        self.exit_code = exit_code

    # Some Validation Rules for Error Handling Strategies:
    def __post_init__(self):
        # you can't return error itself & fallback value at the same time
        if not self.blacklist and not self.whitelist: # when list = False or [] 
            raise ValueError("Either set blacklist or whitelist. Both can't be empty.")
        
        if self.blacklist and self.whitelist:  # when list = False or []
            raise ValueError(
                "Blacklist or Whitelist can not be set to 'True' at the same time. It means catch all and skip all.")
        # you can't whitelist and blacklist the same exception at the same time, so no intersection.
        any_intersection = list(set(self.blacklist) & set(self.whitelist))
        if any_intersection:
            raise ValueError(
                "You can not have the same exception in both blacklist & whitelist.")

    def __check_if_blacklisted__(self, exception):
        if self.blacklist:
            if isinstance(self.blacklist, List):
                for exception_type in self.blacklist:
                    if isinstance(exception, exception_type):
                        return True
            else:
                return True

        return False

    def __check_if_whitelisted__(self, exception):
        if self.whitelist:
            if isinstance(self.whitelist, List):
                for exception_type in self.whitelist:
                    if isinstance(exception, exception_type):
                        return True
            else:
                return True

        return False

    @abstractmethod
    def execute(self, callback: Function): 
        try:
            return callback.call()
        except Exception as exception:
            did_error_occur = self.__check_if_blacklisted__(exception)
            skippable_error = self.__check_if_whitelisted__(exception)
            self.on_error(exception)
            
            # if blacklisted
            if did_error_occur:
                if self.exit_code != -1:
                    import sys
                    sys.exit(self.exit_code)
                else:
                    raise exception # reraise

            # if whitelisted skip
            if skippable_error:
                return self.fallback_value
    
    # Below are for organization and readbility purposes only
    class AbstractStrategy:
        def execute(self, callback: Function): ...

    class JustHandle(AbstractStrategy):
        ...

    class ReturnError(AbstractStrategy):
        ...

    class NeverHandle(AbstractStrategy):
        ...

    class ReturnValue(AbstractStrategy):
        ...

    class ReturnValueOrError(AbstractStrategy):
        ...

class NoHandle(ErrorStrategy):
    
    def __init__(self) -> None: 
        super().__init__(blacklist=True,
                         whitelist=False)

    def execute(self, callback: Function):
        return super().execute(callback)


class JustHandle(ErrorStrategy):
    def __init__(self,
                 on_error: Function = default_on_error,
                 exit_code: int = -1) -> None:
        super().__init__(on_error=on_error,
                         exit_code=exit_code,
                         blacklist=False,
                         whitelist=True)

    def execute(self, callback: Function):
        return super().execute(callback)

class ReturnError(ErrorStrategy):
    def __init__(self,
                 on_error: Function = default_on_error) -> None:
        super().__init__(blacklist=True,
                         whitelist=False,
                        on_error=on_error)

    def execute(self, callback: Function):
        try:
            return super().execute(callback)
        except Exception as exception:
            return exception


class ReturnValue(ErrorStrategy):
    def __init__(self, 
                 fallback_value: Any = None,
                 on_error: Function = default_on_error) -> None:
        super().__init__(blacklist=False,
                         whitelist=True,
                         on_error=on_error,
                         fallback_value=fallback_value)

    def execute(self, callback: Function):
        return super().execute(callback)

class ReturnValueOrError(ErrorStrategy):
    # TODO: returns value if whitelisted, returns or reraises if on blacklist
    def __init__(self, blacklist=None, whitelist=None, on_error: Function = None, fallback_value: Any = None) -> None:
        super().__init__(blacklist=blacklist,
                         whitelist=whitelist,
                         on_error=default_on_error,
                         fallback_value=fallback_value)

    def execute(self, callback: Function):
        try:
            return callback.call()
        except Exception as exception:
            did_error_occur = self.__check_if_blacklisted__(exception)
            skippable_error = self.__check_if_whitelisted__(exception)
            self.on_error(exception)

            if did_error_occur:
                return exception

            if skippable_error:
                return self.fallback_value
