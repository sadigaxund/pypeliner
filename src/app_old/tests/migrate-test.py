from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError
import time

BROKER_ADDRESS = 'localhost:9092'
NEW_PARTITION_COUNT = 1
TEMP_TOPIC_NAME = 'temp-migration'

def copy_messages(source_topic, dest_topic):
    consumer = KafkaConsumer(
        source_topic,
        bootstrap_servers=BROKER_ADDRESS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=10000  # Stop iteration if no message after 10 seconds
    )

    producer = KafkaProducer(bootstrap_servers=BROKER_ADDRESS)

    for message in consumer:
        producer.send(dest_topic, key=message.key, value=message.value, timestamp_ms=message.timestamp)

    producer.close()
    consumer.close()

def create_topic_with_config(admin, topic_name, partitions):
    topic = NewTopic(
        name=topic_name,
        num_partitions=1,
        replication_factor=1,  # Assuming a single broker. Adjust for multi-broker setups.
        topic_configs={"retention.ms": 2678400000, "max.message.bytes": 10485760}
    )
    try:
        admin.create_topics([topic])
    except TopicAlreadyExistsError as e:
        print(f"Topic {topic_name} already exists. Error: {e}")

def wait_for_topic_deletion(admin, topic_name, max_retries=10, delay=10):
    for i in range(max_retries):
        topic_list = admin.list_topics()
        if topic_name not in topic_list:
            return True
        time.sleep(delay)
    return False

def main(original_topic):
    admin = KafkaAdminClient(bootstrap_servers=BROKER_ADDRESS)
    
    # Step 1: Fetch configurations of the old topic

    # Step 2: Create temporary topic with old configurations
    create_topic_with_config(admin, TEMP_TOPIC_NAME, NEW_PARTITION_COUNT)

    # Step 3: Copy messages from original topic to temp topic
    copy_messages(original_topic, TEMP_TOPIC_NAME)

    # Step 4: Delete old topic
    admin.delete_topics([original_topic])
    if not wait_for_topic_deletion(admin, original_topic):
        print("Failed to delete the old topic in time. Exiting.")
        return

    # Step 5: Create new topic with reduced partitions and old configurations
    create_topic_with_config(admin, original_topic, NEW_PARTITION_COUNT)

    # Step 6: Copy messages back from temp topic to new topic
    copy_messages(TEMP_TOPIC_NAME, original_topic)

    # Step 7: Clean up - Delete temporary topic
    admin.delete_topics([TEMP_TOPIC_NAME])
    print(f"Migration for {original_topic} complete!")

if __name__ == '__main__':
    original_topic_name = input("Enter the name of the original topic: ")
    main(original_topic_name)