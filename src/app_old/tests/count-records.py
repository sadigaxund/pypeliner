from kafka import KafkaConsumer
import json
import argparse

# Kafka broker details
bootstrap_servers = 'SER6:9092'

def consume_messages(consumer, topic):
    consumer.subscribe([topic])

    key_counts = {}  # Dictionary to store key counts

    for message in consumer:
#        key = message.key.decode('utf-8') if message.key else None
        value = message.value
        if not value['__metadata']['mapped_id']['hotel_id_t']:
            print(value['__metadata']) 

        continue
        if key in key_counts:
            key_counts[key] += 1
        else:
            key_counts[key] = 1

        print(f"\r{key_counts}", end='', flush=True)

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Consume messages from a Kafka topic and count key occurrences.')
        parser.add_argument('topic', type=str, help='Name of the Kafka topic')
        args = parser.parse_args()

        # Configure the Kafka consumer
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',  # Start consuming from the beginning of the topic
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

        # Consume messages from the specified topic
        consume_messages(consumer, args.topic)
    except KeyboardInterrupt:
        print("\nStop Signal Recieved!")
