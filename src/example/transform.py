from pypeliner.tags.process import processor

immutable = True
fallback = 0
forgiving = False

@processor
def add_1(value):
    return value + 7


@processor
def multiple_2(value):
    return value * 4

def not_that_function():...