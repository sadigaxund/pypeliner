

def get_new_decorator():
    
    def mydecorator(func):
        mydecorator.funcs.append(func)
        def wrapper(*args, **kwargs):
            print(mydecorator.funcs)
            return func(*args, **kwargs)
        return wrapper
    mydecorator.funcs = []
    return mydecorator


class MyMeta(type):
    def __new__(cls, name, bases, dct):
        decorator = get_new_decorator()
        for key, value in dct.items():
            if callable(value):
                dct[key] = decorator(value)
        return super().__new__(cls, name, bases, dct)



class myclass:
    ...
    

class newcls(myclass, metaclass=MyMeta):
    def myfunc(self, record):
        print("Function 1 inside newcls 1")
    def myfunc2(self, record):
        print("Function 2 inside newcls 1")
        
class newcls2(myclass, metaclass=MyMeta):
    def myfunc(self, record):
        print("Function 1 inside newcls 2")
    def myfunc2(self, record):
        print("Function 2 inside newcls 2")

# Creating an instance of newcls
newcls().myfunc(123)
newcls2().myfunc(123)
