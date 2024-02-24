from example.misc.client import LoadClient
import atexit
from pypeliner.tags.load import loader
from pypeliner.tags.process import processor
from pypeliner.tags.extract import execute, iterate, available

immutable = True
fallback = 0
forgiving = False
mylist = list(range(10))
index = 0


@execute
def run():
    return mylist[index]

@available
def has_next():
    return index < len(mylist) - 1

@iterate
def iterate():
    global index
    index += 1


@processor
def add_1(value):
    return value + 7


@processor
def multiple_2(value):
    return value * 4


client = LoadClient()
atexit.register(client.close)


@loader
def load(value):
    client.load(value)


@loader
def load2(value):
    print(">> Send Kafka:", value)

