from pypeliner.tags.process import processor

immutable = True
fallback = 0
forgiving = False
raise_error = True

@processor
def add_1(value):
    return value + 7


@processor
def multiple_2(value):
    return value * 4
