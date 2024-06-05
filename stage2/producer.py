from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
import trino
import json
import sys
import os
import urllib3
import socket

# Disable insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set proxy environment variables if needed
#os.environ['http_proxy'] = "http://proxyparfil.si.fr.intraorange:8080"
#os.environ['https_proxy'] = "http://proxyparfil.si.fr.intraorange:8080"

# Kafka bootstrap servers
#kafka_bootstrap_servers = 'localhost:9092'

def create_kafka_topic(topic_name):
    admin_client = AdminClient({'bootstrap.servers': 'kafka:9092'})
    topic_list = [NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)]
    fs = admin_client.create_topics(topic_list)

    # Wait for each operation to finish
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

def connect_to_starburst():
    try:
        conn = trino.dbapi.connect(
            host='stargate-poc.com.intraorange',  # Replace with your Starburst host
            port=443,  # Replace with your Starburst port
            user='yasmine.benabda',  # Replace with your Starburst username
            http_scheme='https',  # 'http' or 'https' if your server uses SSL
            catalog='stargate-kpi',
            auth=trino.auth.BasicAuthentication('yasmine.benabda', 'yasmine123'),  # Replace with your Starburst password
            verify=False
        )
        return conn
    except socket.gaierror as e:
        print(f"Failed to resolve hostname: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Failed to connect to Starburst: {e}")
        sys.exit(1)


def convert_datetime_objects(obj):
    """Recursively convert datetime objects to strings."""
    if isinstance(obj, dict):
        return {key: convert_datetime_objects(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_objects(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

def produce_data(schema, table, kafka_topic):
    # Create Kafka topic
    create_kafka_topic(kafka_topic)

    # Kafka producer
    producer = Producer({'bootstrap.servers': 'kafka:9092'})
    
    # Connect to Starburst and fetch data
    conn = connect_to_starburst()
    cur = conn.cursor()
    query = f"SELECT * FROM {schema}.{table}"
    
    try:
        cur.execute(query)
    except trino.exceptions.TrinoConnectionError as e:
        print(f"Failed to execute query: {e}")
        sys.exit(1)

    # Fetch and produce data to Kafka iteratively
    row_count = 0
    while True:
        row = cur.fetchone()
        if row is None:
            break
        #Debugging: Print original row to see its structure
        #print("Original Row:", row)
     #convert datetime objects to strings
       # for column_name, value in row.items():
        #    if isinstance(value, datetime):
         #       row[column_name] = value.isoformat()  # Convert datetime to ISO 8601 string
       # Debugging: Print processed row to confirm conversion
       # print("Processed Row:", row)
        processed_row = convert_datetime_objects(row)
        data = json.dumps(processed_row)        
        
        producer.produce(kafka_topic, value=data.encode('utf-8'))
        producer.poll(0)
        row_count += 1

    producer.flush()
    print(f"Produced {row_count} rows from {schema}.{table} to Kafka topic {kafka_topic}")

def main():
    if len(sys.argv) < 3:
        print("Usage: producer.py <kafka_topic> <schema.table1> <schema.table2> ...")
        sys.exit(1)

    kafka_topic = sys.argv[1]
    table_names = sys.argv[2:]

    for table_name in table_names:
        try:
            schema, table = table_name.split('.')
            produce_data(schema, table, kafka_topic)
            print(f"Data from table {table_name} produced to Kafka topic {kafka_topic}")
        except Exception as e:
            print(f"Failed to produce data from table {table_name} to Kafka topic {kafka_topic}: {e}")

if __name__ == "__main__":
    main()

