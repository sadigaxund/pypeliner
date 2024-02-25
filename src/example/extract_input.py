from pypeliner.tags.extract import execute, iterate, available, input
from typing import Never


@input
def handle_input(input):
    global INPUT
    INPUT = (el for el in input)

@execute
def run():
    return next(INPUT) + 10


# TODO: Make so that when input is provided, there will be no need for below two functions:
# Because: run function above will raise StopIteration exception whenever no more input left.
# Which in turn will stop wrapper Iterator, because this exception will propogate to __next__ function.

@available
def is_available():
    return True

@iterate
def iterate():
    ...
    
