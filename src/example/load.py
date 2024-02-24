from example.misc.client import LoadClient
from pypeliner.tags.load import loader
import atexit

client = LoadClient()
atexit.register(client.close)

@loader
def load(value):
    client.load(value)


@loader
def load2(value):
    print(">> Send Kafka:", value)
    
