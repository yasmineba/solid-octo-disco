from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import trino
import json
import sys

kafka_bootstrap_servers = 'kafka:9092'

def create_kafka_topic(topic_name):
    admin_client = AdminClient({'bootstrap.servers': kafka_bootstrap_servers})
    
    topic_list = [NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)]
    fs = admin_client.create_topics(topic_list)
    
    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

def produce_data(schema, table, kafka_topic):
    create_kafka_topic(kafka_topic)

    producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})
    conn = connect_to_starburst()
    cur = conn.cursor()

    query = f"SELECT * FROM {schema}.{table}"
    cur.execute(query)
    rows = cur.fetchall()

    for row in rows:
        data = json.dumps(row)
        producer.produce(kafka_topic, data.encode('utf-8'))

    producer.flush()

def connect_to_starburst():
    conn = trino.dbapi.connect(
        host='stargate-poc.com.intraorange',  # Replace with your Starburst host
        port=443,  # Replace with your Starburst port
        user='yasmine',  # Replace with your Starburst username
        http_scheme='https',  # or 'https' if your server uses SSL
        catalog='stargate-kpi',
        auth=trino.auth.BasicAuthentication('yasmine', 'yasmine123'),  # Replace with your Starburst password
        verify=False
    )
    return conn

def main():
    if len(sys.argv) < 3:
        print("Usage: producer.py <kafka_topic> <schema.table1> <schema.table2> ...")
        sys.exit(1)

    kafka_topic = sys.argv[1]
    table_names = sys.argv[2:]

    conn = connect_to_starburst()

    for table_name in table_names:
        schema, table = table_name.split('.')
        produce_data(schema, table, kafka_topic)
        print(f"Data from table {table_name} produced to Kafka topic {kafka_topic}")

if __name__ == "__main__":
    main()
