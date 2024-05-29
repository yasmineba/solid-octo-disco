from kafka import KafkaProducer

# Create an instance of the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))

# Send some messages
producer.send('mytopic', 'Hello kafka')

# Ensure all messages have been sent
producer.flush()

# Close the producer connection
producer.close()
