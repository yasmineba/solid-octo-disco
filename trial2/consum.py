from kafka.admin import KafkaAdminClient
from kafka import KafkaConsumer
import json

# Configuration
bootstrap_servers = 'localhost:9092'

def list_topics():
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin_client.list_topics()
    admin_client.close()
    return topics

def display_topic_details(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest', enable_auto_commit=False)
    
    # Fetching metadata and configuration details (if required)
    partitions = consumer.partitions_for_topic(topic)
    if partitions is None:
        print(f"Topic '{topic}' does not exist.")
        return
    
    topic_metadata = consumer.topics()
    
    # Display details
    print(f"Details for topic '{topic}':")
    print(f"Partitions: {partitions}")
    print(f"Metadata: {json.dumps(topic_metadata[topic], indent=2)}")
    
    # Optionally, display some messages from the topic
    print("\nFetching some messages from the topic...")
    for i, message in enumerate(consumer):
        print(f"Offset: {message.offset}, Key: {message.key}, Value: {message.value}")
        if i >= 5:  # Limit the number of messages displayed
            break
    
    consumer.close()

def main():
    topics = list_topics()
    print("Available topics:")
    for topic in topics:
        print(f" - {topic}")
    
    topic_name = input("\nEnter the topic name you want to display details for: ")
    display_topic_details(topic_name)

if __name__ == "__main__":
    main()
