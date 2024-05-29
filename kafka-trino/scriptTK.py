import requests
from confluent_kafka import Producer
import json

# Configurations
TRINO_URL = "http://localhost:8080"
TRINO_CATALOG = "tpch"
TRINO_SCHEMA = "tiny"
TRINO_TABLE = "customer"
KAFKA_TOPIC = "mytopic"
KAFKA_BROKER = "localhost:9092"

# Kafka Producer Configuration
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Function to handle delivery reports from Kafka
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Function to query data from Trino
def query_trino(query):
    response = requests.post(
        f"{TRINO_URL}/v1/statement",
        data=query,
        headers={
            'X-Trino-User': 'user',
            'X-Trino-Catalog': TRINO_CATALOG,
            'X-Trino-Schema': TRINO_SCHEMA
        }
    )
    
    if response.status_code == 200:
        result = response.json()
        print("Initial Query Result:", result)  # Debug information

        # Handling pagination for large result sets
        while 'nextUri' in result:
            next_uri = result['nextUri']
            response = requests.get(next_uri)
            if response.status_code == 200:
                result = response.json()
                print("Paginated Query Result:", result)  # Debug information
            else:
                print("Failed to fetch next page of results:", response.content)
                break
        
        return result
    else:
        print("Failed to query Trino: ", response.content)
        return None

# Query to fetch data from Trino
query = f"SELECT * FROM {TRINO_TABLE}"

# Fetch data from Trino
result = query_trino(query)
print("Final Result:", result)  # Final result after pagination

if result and 'data' in result:
    for row in result['data']:
        # Convert row to JSON string
        message = json.dumps(row)
        
        # Produce message to Kafka
        producer.produce(KAFKA_TOPIC, message, callback=delivery_report)
        producer.flush()
else:
    print("No data fetched from Trino or error occurred.")

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()

