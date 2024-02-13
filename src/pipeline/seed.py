
from typing import NoReturn, Callable, Optional, Any
# Seed has behavior: 
# * more        - tells if there might be next phase
# * iterate     - returns new seed with updated parameters
# * grow        - executes based on given parameters & saves result 

# Steps:
# * first time: runs, returns result, sets state
# * every other: checks state, if has more then run again, reset state
# # if no more: then return stop iteration


def execute(func):
    func.udf_execute = True
    return func

def iterate(func):
    func.udf_iterate = True
    return func

def more(func):
    func.udf_more = True
    return func

class Seed:
    #############
    # MAIN
    #############

    def __init_from_module__(self, module):
        def get_function(module, attribute):
            for func_name in dir(module):
                if hasattr(getattr(module, func_name), attribute):
                    return getattr(module, func_name)
                
        self.execute = get_function(module, 'udf_execute')
        self.iterate = get_function(module, 'udf_iterate')
        self.more = get_function(module, 'udf_more')
        
        
    def __init__(self, module = None) -> NoReturn:
        self.__any_more = True
        
        if module:
            self.__init_from_module__(module)        
        
    
    def __iter__(self):
        return self

    def __next__(self):
        if not self.__any_more:
            raise StopIteration
        
        result = self.execute()
        if self.more:
            self.iterate()
        
        return result

    #############
    # INNER
    #############

    def __execute__(self): 
        raise NotImplementedError("Implement 'grow' behavior to plant the seed!")
    
    def __more__(self): 
        raise NotImplementedError("Implement 'more' behavior to plant the seed!")
    
    def __iterate__(self): 
        raise NotImplementedError("Implement 'iterate' behavior to plant the seed!")
 
    #############
    # PUBLIC
    #############

    @property
    def execute(self) -> Callable:
        return self.__execute__

    @execute.setter
    def execute(self, new_behaviour: Callable) -> NoReturn:
        self.__execute__ = new_behaviour

    @property
    def more(self) -> bool:
        self.__any_more = self.__more__()
        return self.__any_more

    @more.setter
    def more(self, new_behaviour: Callable) -> NoReturn:
        self.__more__ = new_behaviour
    
    @property
    def iterate(self) -> Callable:
        return self.__iterate__

    @iterate.setter
    def iterate(self, new_behaviour: Callable) -> NoReturn:
        self.__iterate__ = new_behaviour
    


