# mymodule.py

def run(func):
    func.is_runnable = True
    return func


def iterate(func):
    func.is_iterable = True
    return func


def has_next(func):
    func.has_next_function = True
    return func

# Define your functions here


@run
def somename():
    print("Executing somename")


@iterate
def someothername():
    print("Iterating someothername")


@has_next
def thirdname():
    print("Checking for next thirdname")
