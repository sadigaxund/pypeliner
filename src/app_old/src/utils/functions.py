
from numpy import deprecate
from .logs import printlog, DEFAULT_LOG_SEPERATOR as __spr__
from functools import wraps
import traceback
import inspect
import time
import json
import sys

__all__ = [
    "FALLBACK_VALUE",
    "handle",
    "handle",
    "trace",
    "equals",
    "get_module_name",
    "coalesce",
    "safe_divide",
    "safe_convert_to_json",
    "custom_encoder",
    "deduplicate"
]

FALLBACK_VALUE = 0x1A3F07B2D8

# Custom encoder to handle non-serializable objects


def deduplicate(iterable):
    seen = set()
    deduplicated = []

    for item in iterable:
        item_repr = tuple(sorted(item.items())) if isinstance(
            item, dict) else item
        if item_repr not in seen:
            seen.add(item_repr)
            deduplicated.append(item)

    return deduplicated

def custom_encoder(obj):
    try:
        # Try to serialize the object using the default JSON encoder
        return json.dumps(obj)
    except TypeError:
        # If serialization fails, convert the object to a string
        return str(obj)

def safe_convert_to_json(msg):
    try:
        val = json.loads(msg)
        return val
    except json.JSONDecodeError as e:
        return {'message' : str(msg)}

def safe_divide(dividend, divisor):
    if not divisor or not dividend:
        return -1  # You can return None or raise an exception if you prefer
    return dividend / divisor


def coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg
    return None



def get_module_name() -> str:
    """
    Get the name of the module that called this function.
    """
    # get directory seperator based on OS
    sep = "\\" if sys.platform == "win32" else "/"

    caller_frame = inspect.stack()[1]  # Get the calling frame
    caller_module = inspect.getmodule(
        caller_frame[0])  # Get the module of the caller
    module_path = inspect.getabsfile(caller_module)
    module_file = module_path.split(sep)[-1].split(".")[0]
    return str(module_file)



def equals(v1: any, v2: any) -> bool:
    try:
        if type(v1) != type(v2):
            raise TypeError()

        return v1 == v2
    except (TypeError, ValueError, Exception):
        return False



def trace(message: str = None, fallback_value: any = FALLBACK_VALUE):
    """
    Log the start and finish of the decorated function.

    Parameters
    ----------
        log_message (str, optional): A custom log message. If not provided,
            the decorator will automatically generate a log message using the
            function's name and arguments. Default is None.
        fallback_value (any, optional): The fallback value to compare the return
            value of the function against. If the function returns this value,
            the finish log will not be generated. Default is None.

    Returns
    -------
        callable: The decorated function.

    Example:
        @logger(log_message="Custom log message")
        def my_function(x, y):
            return x + y

        result = my_function(3, 5)
        # Output:
        # INFO:__main__:Started: Custom log message
        # INFO:__main__:Finished: Custom log message
    """
    def decorator(func):
        @wraps(func)
        def inner(*args, **kwargs):
            nonlocal message
            # merge args and kwargs into a single dictionary
            if not message:
                res_args = args + tuple(kwargs.values())
                message = f"{func.__name__}{res_args}"

            printlog(f"STARTED{__spr__}{message}", "trace")
            f = func(*args, **kwargs)

            if not equals(f, fallback_value):
                printlog(f"FINISHED{__spr__}{message}", "trace")
            return f

        return inner

    return decorator



@deprecate
def handler(
    error_message: str = "Error has occurred:", 
    strict: bool = True, 
    max_retries: int = 3, 
    timeout_secs: int = 3,
    fallback_value: any = None, 
    on_error: callable = None, 
    exit_signal: int = None, 
    do_raise: bool = False, 
):
    def decorator(func):
        def inner(*args, **kwargs):
            nonlocal max_retries
            try:
                error_msg = f"Attempt 1/{max_retries}{__spr__}Could not execute the first try."
                ex = None
                retries = 0
                while retries < max_retries:
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        if strict:
                            # TODO: sometime later
                            # Im too scared to change this to: max_retries = 1. [for now works :D]
                            retries = max_retries
                        if retries < max_retries:
                            printlog(error_msg + f"{__spr__}{str(e)}", "warning")
                            error_msg = f"Attempt {retries + 2}/{max_retries}{__spr__}Retrying... "
                            retries += 1
                            # backoff waiting
                            time.sleep(timeout_secs ** retries)
                        ex = e
                else:
                    raise ex

            except Exception as e:
                # FA

                if exit_signal:
                    sys.exit(exit_signal)
                
                if do_raise:
                    raise e
                return fallback_value if fallback_value else e
        return inner
    return decorator


def handle(
    error_message: str = "An error occurred",
    strict: bool = False,
    fallback_value: any = None,
    on_error: callable = None,
    exit_signal: int = None,
    do_raise: bool = False,
    max_retries: int = 3,
    timeout_secs: int = 3
):
    """
    Decorator function to handle errors.

    Use Cases:
    1. If no error occurs, the function will run normally.
    2. If an error occurs, the function will retry for a maximum of max_retries times.
    3. If an error occurs and max_retries is exceeded, the function will return the fallback_value.
    4. If an error occurs and strict is set to True, the function will raise the error without retrying.
    5. If an error occurs and strict is set to False, the function will print the error and retry.
    6. If an error occurs and strict is set to False and max_retries is exceeded, the function will return the fallback_value.

    Args:
    ----
        message (str): The message to be printed when an error occurs.
        strict (bool): If True, the function will raise the error without retrying.
        max_retries (int): The maximum number of retries.
        fallback_value (any): The value to be returned if an error occurs and max_retries is exceeded.
        on_error (callable): Params[e, *args, **kwargs]. A function to be called when an error occurs. Will recieve metadata of exception, as well as, original parameters of the function
        exit_signalINT (bool): If True, the program will exit with exit code 1.
            Exit signal definitions:
            0: Success
            1: Unexpected System Error
            2: Posible Source Error
            3: Manual Termination
        do_raise (bool): If True, the function will raise the error.
        timeout_secs (int): The number of seconds to wait before retrying. The waiting time will increase exponentially with each retry.

    # TODO: add fallback_yield
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # nonlocal max_retries, message
            attempts = 0

            while True:
                try:
                    attempts += 1
                    return func(*args, **kwargs)
                except Exception as e:
                    metadata = {
                        "function": func.__name__,
                        "exception": e,
                        "traceback": traceback.format_exc()
                    }

                    if attempts > max_retries or strict == True:
                        # Execute Error callback if provided
                        if on_error:
                            on_error()

                        # Log the Error
                        printlog(message=error_message,
                                 level="error", payload=metadata)

                        # Exit with provided signal code
                        if exit_signal:
                            printlog(
                                f"Exiting with exit code: {exit_signal}", "warning")
                            exit(exit_signal)

                        # superseded by exit_signal. will be ignored if exit_signal is provided
                        if do_raise:
                            raise metadata['exception']

                        return fallback_value
                    else:
                        message = f"Attempt {attempts}/{max_retries}"
                        message += f"{__spr__}Could not execute the first try." if attempts == 1 else f"{__spr__}Retrying..."
                        printlog(message=message, level="warning",
                                 payload=metadata)
                    time.sleep(timeout_secs ** attempts)

        return wrapper

    return decorator

