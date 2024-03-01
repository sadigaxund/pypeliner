def __create_function():
    from typing import Union, List, Any
    
    global Function

    class Function:
        ThatDoesNothing: 'Function' = None

        def __init__(self, callback, *args, **kwargs) -> None:
            self.callback = callback
            self.args = args
            self.kwargs = kwargs

        def call(self) -> Any:
            self.callback(*self.args, **self.kwargs)

        def __call__(self) -> Any:
            self.callback(*self.args, **self.kwargs)
    
    Function.ThatDoesNothing = Function(lambda: None)

def __create_task():
    from .policies import RetryPolicy, ErrorStrategy
    global Task
    class Task:
        def __init__(self,
                     function: Function,
                     retry_policy: RetryPolicy = RetryPolicy.Idempotent(),
                     error_strategy: ErrorStrategy = ErrorStrategy.NeverHandle(),
                     name=None,
                     description=None,
                     ) -> None:
            self.function = function
            self.error_policy = error_strategy
            self.retry_policy = retry_policy
            self.name = name
            self.description = description

        def start(self):
            self.retry_policy.attempt(self.function, self.error_policy)

        def stop(self):     ...
        def pause(self):    ...
        def resume(self):   ...
        @classmethod
        def create(function: Function,
                   retry_policy: RetryPolicy = RetryPolicy.Idempotent(),
                   error_strategy: ErrorStrategy = ErrorStrategy.NeverHandle(),
                   name=None,
                   description=None,
                   ) -> None: ...
        
    from functools import wraps

    def create(name=None, description=None, retry_policy=None, error_strategy=None):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                new_func = Function(func, *args, **kwargs)
                task = Task(
                    name=name,
                    description=description,
                    function=new_func,
                    error_strategy=error_strategy,
                    retry_policy=retry_policy
                )
                return task.start()
            return wrapper
        return decorator
    
    Task.create = create
    
def __create_taskline():
    global Taskline
    class Taskline: ...

def __abstract_imports():
    __create_function()
    __create_task()

if __name__ == 'tasker.structs':
    __abstract_imports()
