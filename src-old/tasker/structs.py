from typing import Any

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
            return self.callback(*self.args, **self.kwargs)

        def __call__(self) -> Any:
            return self.call()
    
    Function.ThatDoesNothing = Function(lambda: None)
    return Function

def __create_task():
    from .policies import ErrorStrategy, RetryPolicy
    from functools import wraps
    global Task
    class Task:
        def __init__(self,
                     function: Function,
                     retry_policy = RetryPolicy.Idempotent(),
                     error_strategy = ErrorStrategy.NeverHandle(),
                     name=None,
                     description=None,
                     ) -> None:
            self.function = function
            self.error_policy = error_strategy
            self.retry_policy = retry_policy
            self.name = name
            self.description = description

        def start(self):
            return self.retry_policy.attempt(self.function, self.error_policy)

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

        @classmethod
        def make(function: Function,
                   retry_policy: RetryPolicy = RetryPolicy.Idempotent(),
                   error_strategy: ErrorStrategy = ErrorStrategy.NeverHandle(),
                   name=None,
                   description=None,
                   ) -> None: ...
        
        class Pipeline:...

        @classmethod
        def enqueue(pipeline: Pipeline,
                    function: Function,
                    retry_policy: RetryPolicy = RetryPolicy.Idempotent(),
                    error_strategy: ErrorStrategy = ErrorStrategy.NeverHandle(),
                    name=None,
                    description=None,
                    ) -> None: ...
        
        

    def make(name=None, description=None, retry_policy=None, error_strategy=None):
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
                return task
            return wrapper
        return decorator
    
    Task.create = create
    Task.make = make
    return Task

    
def __create_pipeline():
    global Pipeline, Task
    from collections import deque
    from functools import wraps
    class Pipeline:
        def __init__(self, skip_on_error = False, *tasks: Task) -> None:
            self.tasks = deque(tasks)
            
        def run(self):
            for task in self.tasks:
                task.start()

        def __call__(self) -> Any:
            self.run()
        
        def join(self, task: Task):
            self.tasks.append(task)
            
        def __add__(self, task: Task):
            if isinstance(task, Task):
                self.join(task)
            else:
                raise TypeError("You can only add Tasks to the Pipeline.")
            return self
    
    # FIXME: misimplementation: When using decorator it adds task to pipeline's queue.
    # But then
    def enqueue(pipeline: Pipeline, name=None, description=None, retry_policy=None, error_strategy=None):
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
                pipeline.join(task)
                return None
            return wrapper()
        return decorator
        
    Task.enqueue = enqueue
    return Pipeline
    
def __abstract_imports():
    __create_function()
    __create_task()
    # __create_pipeline()


if __name__.endswith('pypeliner.tasker.structs'):
    __abstract_imports()
