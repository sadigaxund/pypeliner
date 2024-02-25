from kafka import KafkaConsumer, KafkaProducer

BROKER_ADDRESS = 'localhost:9092'


def backup_topic(original_topic_name, new_topic_name, new_key: str):
    # Create a Kafka consumer to fetch messages from the original topic
    consumer = KafkaConsumer(
        original_topic_name,
        bootstrap_servers=BROKER_ADDRESS,
        auto_offset_reset='earliest',  # Start from the beginning of the topic
        enable_auto_commit=False,
        consumer_timeout_ms=10000  # Stop iteration if no message after 10 seconds
    )

    # Create a Kafka producer to send messages to the backup topic
    producer = KafkaProducer(bootstrap_servers=BROKER_ADDRESS)

    # Copy messages from the original topic to the backup topic
    for message in consumer:
        producer.send(new_topic_name, key=new_key.encode("utf-8"), value=message.value)

    producer.close()
    consumer.close()

    print(f"Backup of {original_topic_name} to {new_topic_name} completed!")

if __name__ == '__main__':
    backup_topic(input("Enter the name of the original topic to backup: "), 
                 input("Enter the name of the new topic to backup: "),
                 input("Enter new key: "))
