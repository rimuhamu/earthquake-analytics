import time
import requests
from confluent_kafka import Producer
import json

KAFKA_CONF = {'bootstrap.servers': 'localhost:9092'}
TOPIC = 'earthquakes_live'
USGS_URL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'

producer = Producer(KAFKA_CONF)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")

try:
    while True:
        resp = requests.get(USGS_URL, timeout=10)
        data = resp.json()
        for feat in data.get('features', []):
            record = {
                'id': feat['id'],
                'time': feat['properties']['time'],
                'mag': feat['properties']['mag'],
                'place': feat['properties']['place'],
                'geometry': feat['geometry']
            }
            producer.produce(TOPIC, json.dumps(record).encode('utf-8'), callback=delivery_report)
        producer.flush()
        time.sleep(60)
except KeyboardInterrupt:
    print("Producer stopped")