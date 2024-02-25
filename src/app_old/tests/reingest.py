import sys
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

def read_messages(consumer, topic):
    messages = []
    for message in consumer:
        if message.topic == topic:
            break
        messages.append(message)
    return messages


def create_temp_topic(admin_client, temp_topic, partitions, replication_factor):
    temp_topic_config = {
        "cleanup.policy": "compact",
        # Add other configurations as needed
    }
    admin_client.create_topics([NewTopic(temp_topic, num_partitions=partitions,
                               replication_factor=replication_factor, config=temp_topic_config)])


def recreate_topic(admin_client, topic, partitions, replication_factor, config):
    admin_client.delete_topics([topic])
    admin_client.create_topics([NewTopic(
        topic, num_partitions=partitions, replication_factor=replication_factor, config=config)])


def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <original_topic>")
        sys.exit(1)

    original_topic = sys.argv[1]
    bootstrap_servers = 'SER6:9092'
    temp_topic = 'temp_topic'

    consumer = KafkaConsumer(
        original_topic, 
        bootstrap_servers=bootstrap_servers, 
        group_id='temp_group',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=10000)
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    # Step 1: Read messages from the original topic
    messages = read_messages(consumer, original_topic)

    # Step 2: Create a temporary topic and load messages into it
    topic_info = admin_client.describe_configs([original_topic])[
        original_topic]
    partitions, replication_factor, original_topic_config = topic_info[
        'num_partitions'], topic_info['replication_factor'], topic_info['config']
    create_temp_topic(admin_client, temp_topic, partitions, replication_factor)
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    for message in messages:
        producer.send(temp_topic, key=message.key, value=message.value)

    # Step 3: Delete the old topic and recreate it with the same configurations
    recreate_topic(admin_client, original_topic, partitions,
                   replication_factor, original_topic_config)

    # Step 4: Load records from the temporary topic back into the original topic
    messages = read_messages(consumer, temp_topic)
    for message in messages:
        producer.send(original_topic, key=message.key, value=message.value)

    # Cleanup: Delete the temporary topic
    admin_client.delete_topics([temp_topic])


if __name__ == "__main__":
    main()
