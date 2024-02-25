import argparse
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def ensure_kafka_topic(bootstrap_servers, topic_name: str, partitions=1, replication=1, config: dict = None) -> str:
    # Initialize the Kafka Admin Client
    topic_was_created = __create_kafka_topic(bootstrap_servers=bootstrap_servers,
                                             topic_name=topic_name,
                                             partitions=partitions,
                                             replication=replication,
                                             config=config)
    if topic_was_created:
        return (f"Topic created successfully. || Topic: {topic_name}")
    else:
        return (f"Topic already exists. || Topic: {topic_name}")

def __create_kafka_topic(bootstrap_servers, topic_name, partitions=3, replication=1, config=None) -> bool:
    """
    Create a Kafka topic based on the provided configurations in VARS.
    """
    # Create a KafkaAdminClient instance
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    topic = NewTopic(name=topic_name, num_partitions=partitions,
                     replication_factor=replication, topic_configs=config)

    # Create topics using the admin client
    try:
        admin_client.create_topics(new_topics=[topic])
        return True
    except TopicAlreadyExistsError:
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create or check if a Kafka topic exists.")
    parser.add_argument("topic_name", type=str, help="Name of the Kafka topic")

    args = parser.parse_args()
    bootstrap_servers = "localhost:9092"  # Update with your Kafka broker's address
    topic_name = args.topic_name
    print(ensure_kafka_topic(bootstrap_servers, topic_name))
