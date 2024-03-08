
def __create_error_strategy__():
    global ErrorStrategy
    from .error import ErrorStrategy, JustHandle, ReturnError, NeverHandle, ReturnValue, ReturnValueOrError
    ErrorStrategy.JustHandle = JustHandle
    ErrorStrategy.ReturnError = ReturnError
    ErrorStrategy.NeverHandle = NeverHandle
    ErrorStrategy.ReturnValue = ReturnValue
    ErrorStrategy.ReturnValueOrError = ReturnValueOrError
    return ErrorStrategy

def __create_retry_policy__():
    global RetryPolicy
    from .retry import RetryPolicy, Exponential, Linear, Idempotent, Selective
    RetryPolicy.Selective = Selective
    RetryPolicy.Exponential = Exponential
    RetryPolicy.Linear = Linear
    RetryPolicy.Idempotent = Idempotent
    return RetryPolicy
    
def __abstract_imports():
    __create_error_strategy__()
    __create_retry_policy__()

if __name__.endswith('pypeliner.tasker.policies'):
    __abstract_imports()
