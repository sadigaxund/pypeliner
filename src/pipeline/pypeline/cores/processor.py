
from ..types import *
from typing import NoReturn, Callable
from copy import deepcopy
# Seed has behavior:
# * available        - tells if there might be next phase
# * iterate     - returns new seed with updated parameters
# * grow        - executes based on given parameters & saves result

# Steps:
# * first time: runs, returns result, sets state
# * every other: checks state, if available then run again, reset state
# # if no more: then return stop iteration

no_change = lambda x: x

class ProcessorCore:
    #############
    # MAIN
    #############

    def __init__(self, 
                 *udf_transformers: Callable,
                 immutable=False,    # decides if process affect the value or not
                 forgiving=False,   # decides if process should be forgiving the error
                 fallback = None,   # this value will returned if process is not forgiving
                 ) -> NoReturn:
        self.__processors = (no_change,) + udf_transformers
        self.clone = deepcopy if immutable else no_change
        self.forgiving = forgiving
        self.fallback = fallback

        
    def process(self, value: Any) -> Any:
        # Copy or not the value        
        retval = self.clone(value)
        for processor in self.__processors:
            # Apply every processor on the value
            try:
                retval = processor(retval)
            except Exception as e:
                if self.forgiving:
                    continue
                else:
                    return self.fallback
        return retval