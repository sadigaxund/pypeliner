def fn2key(fn):
    return tuple([
            fn.__module__,
            fn.__class__,
            fn.__name__,
        ])

class MultiFunction(object):
    def __init__(self, *args):
        self.functions = list()
        self.functions.extend(args)
        
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
        

    def append(self, new_fn):
        self.functions.append(new_fn)
        
    def extend(self, other: 'MultiFunction'):
        return self.functions.extend(other.functions)
        
    
    # def __repr__(self) -> str:
    #     if len(self.functions) > 0:
    #         return self.functions[0]
    #     else:
    #         return None
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
        self.function_map = dict()
        
    def get(self, fn: MultiFunction) -> MultiFunction:
        return self.function_map.get(fn.key())
    
    def set(self, fn) -> MultiFunction:
        self.function_map[fn.key()] = fn
    
    def register(self, fn):
        fn = MultiFunction(fn)
        exists = self.get(fn)
        final_form = None
        if exists:
            exists.extend(fn)
            final_form = exists
        else:
            final_form = fn
            
        self.set(final_form)
        return final_form