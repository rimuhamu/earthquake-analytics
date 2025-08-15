import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pymongo import MongoClient

# Kafka consumer configuration
CONSUMER_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'earthquake-consumer',
    'auto.offset.reset': 'earliest'
}
TOPIC = 'earthquakes_live'

# Cassandra configuration
CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'earthquakes'
CASSANDRA_TABLE = 'events'

# MongoDB configuration
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'earthquakes'
MONGO_COLLECTION = 'events'

# Establish database connections
try:
    # Connect to Cassandra
    cluster = Cluster(CASSANDRA_CONTACT_POINTS)
    session = cluster.connect()
    insert_cql = SimpleStatement(
        f"INSERT INTO {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} (id, time, mag, place, lon, lat, depth) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )

    # Connect to MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    mongo_collection = mongo_db[MONGO_COLLECTION]

except Exception as e:
    print(f"Database connection error: {e}")
    exit(1)

# Initialize Kafka consumer
consumer = Consumer(CONSUMER_CONF)

try:
    consumer.subscribe([TOPIC])
    print(f"Subscribed to Kafka topic: {TOPIC}")

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                continue

        try:
            record = json.loads(msg.value().decode('utf-8'))

            mongo_collection.insert_one(record)

            coords = record.get('geometry', {}).get('coordinates', [None, None, None])
            event_time = datetime.utcfromtimestamp(record.get('time') / 1000)

            session.execute(
                insert_cql,
                (record.get('id'), event_time, record.get('mag'), record.get('place'), coords[0], coords[1], coords[2])
            )
            print(f"Successfully processed event: {record.get('id')} at {event_time}")

        except Exception as e:
            print(f"Data processing or database insert error: {e}")

except KeyboardInterrupt:
    print("Consumer stopped by user.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

finally:
    consumer.close()
    cluster.shutdown()
    mongo_client.close()
    print("Connections closed.")