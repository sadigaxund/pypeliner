from .functions import trace, handle, custom_encoder
from .constants import MSG_ERROR_INIT_KAFKA_CLIENT
from .logs import printlog
from confluent_kafka import Consumer, Producer, KafkaException
import socket
import json

__all__ = [
    "get_or_create_kafka_producer",
    "get_or_create_kafka_consumer",
    
    "default_value_serializer",
    "default_value_deserializer",
    "default_key_serializer",
    "default_key_deserializer",
]
GROUP_ID = "validator-group"
_health_check_topic = "health-check-topic"
_existing_producer: Producer = None
_existing_consumer: Consumer = None


class KafkaConsumer:
    def __init__(self, consumer):
        self.consumer = consumer

    def __enter__(self):
        return self.consumer

    def __exit__(self, exc_type, exc_value, traceback):
        # Close the consumer when exiting the context
        self.consumer.close()


class KafkaProducer:
    def __init__(self, producer):
        self.producer = producer

    def __enter__(self):
        return self.producer

    def __exit__(self, exc_type, exc_value, traceback):
        # Close the producer when exiting the context
        self.producer.flush()
    
    def flush(self):
        self.producer.flush()
        
    def close(self):
        self.producer.close()

    def produce(self, topic, key, value, callback):
        # Send the message to the specified topic
        self.producer.produce(topic=topic,
                              key=key,
                              value=value,
                              callback=callback)




########################################################
## AUXILARY FUNCTIONS
########################################################

def default_value_serializer(x):
    return json.dumps(x, ensure_ascii=False, default=custom_encoder).encode('utf-8')


def default_value_deserializer(encoded_data):
    return json.loads(encoded_data.decode('utf-8'))


def default_key_serializer(x):
    return str(x).encode('utf-8')


def default_key_deserializer(encoded_data):
    return encoded_data.decode('utf-8')



def is_producer_working(producer):
    try:
        producer.produce(_health_check_topic,
                         key=default_key_serializer("health-check"),
                         value=default_value_serializer({"message": "test-message"}))
        producer.flush()
        return True
    except Exception as e:
        print(f"Producer health check failed: {e}")
        return False

########################################################
# MAIN FUNCTIONS
########################################################

def _create_kafka_consumer(broker: str, group_id, **kwargs):
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "auto.offset.reset": "earliest",
            "group.id": group_id,
            'enable.auto.commit': False
        },
    )
    return consumer


def _create_kafka_producer(broker: str, **kwargs) -> Producer:
    return Producer(
        {
            'client.id': socket.gethostname(),
            "bootstrap.servers": broker,
            "request.timeout.ms": 60000,
            "delivery.timeout.ms": 60000,
            "message.max.bytes": 1048576,
            "retries": 5
        }
    )


@handle(error_message=MSG_ERROR_INIT_KAFKA_CLIENT, strict=False, do_raise=True)
def get_or_create_kafka_producer(broker_url: str) -> KafkaProducer:
    global _existing_producer
    
    if _existing_producer is None:
        _existing_producer = _create_kafka_producer(broker=broker_url)
    
    if is_producer_working(_existing_producer):
        return KafkaProducer(_existing_producer)
    else:
        raise KafkaException("Unable to establish connection with Kafka brokers using the new producer.")


@trace(message="Initializing Kafka Consumer Client.")
@handle(error_message=MSG_ERROR_INIT_KAFKA_CLIENT, strict=False, do_raise=True)
def get_or_create_kafka_consumer(broker_url: str, topic_name, group_id = GROUP_ID) -> KafkaConsumer:
    global _existing_consumer
    
    if _existing_consumer is None:
        _existing_consumer = _create_kafka_consumer(
            broker=broker_url, group_id=group_id)
    
    _existing_consumer.subscribe(topics=[topic_name])
    return KafkaConsumer(_existing_consumer)



    

