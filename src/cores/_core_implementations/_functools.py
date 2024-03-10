from typing import Callable
def fn2key(fn):
    return tuple([
            fn.__module__,
            fn.__class__,
            fn.__name__,
        ])

class MultiFunction(object):
    def __init__(self, fn = None):
        self.functions = list()
        
        if isinstance(fn, MultiFunction):
            self.extend(fn)
        elif isinstance(fn, Callable):
            self.functions.append(fn)
        
    def __call__(self, *args, **kwargs):
        result = None
        
        for fn in self.functions:
            try:
                result = fn(*args, **kwargs)
                kwargs['record'] = result
            except TypeError as e:
                if "got an unexpected keyword argument 'record'" in str(e):
                    raise KeyError("Marked methods should have 'record' as a keyword arguement/parameter.") from None
                else:
                    raise e
        return result
    
    
    def __add__(self, new_fn):
        if not isinstance(new_fn, Callable):
            raise TypeError("Can't append anything other than object of type Callable.")
        
        if isinstance(new_fn, MultiFunction):
            self.functions.extend(new_fn.functions)
        else:
            self.functions.append(new_fn)
        
        return self
 
    def __str__(self) -> str:
        return str(self.functions)
        
    def key(self):
        fn = self.functions[0] if len(self.functions) > 0 else None
        
        if fn:
            return fn2key(fn)
        else:
            return tuple()


class CoreNamespace:
    def __init__(self) -> None:
        self.functions = MultiFunction()

    def register(self, fn):
        self.functions += fn
        return self.functions
