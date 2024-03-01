def __abstract_imports():
    global ErrorStrategy
    from .error import ErrorStrategy, JustHandle, ReturnError, NoHandle, ReturnValue, ReturnValueOrError
    ErrorStrategy.JustHandle = JustHandle
    ErrorStrategy.ReturnError = ReturnError
    ErrorStrategy.NeverHandle = NoHandle
    ErrorStrategy.ReturnValue = ReturnValue
    ErrorStrategy.ReturnValueOrError = ReturnValueOrError
    
    global RetryPolicy
    from .retry import RetryPolicy, Exponential, Linear, Idempotent
    RetryPolicy.Exponential = Exponential
    RetryPolicy.Linear = Linear
    RetryPolicy.Idempotent = Idempotent
    

if __name__ == 'tasker.policies':
    __abstract_imports()