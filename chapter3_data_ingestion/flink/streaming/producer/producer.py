import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker

fake = Faker()

def create_producer():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    retry_attempts = int(os.getenv('KAFKA_RETRY_ATTEMPTS', '5'))
    retry_delay = int(os.getenv('KAFKA_RETRY_DELAY', '10'))
    
    for attempt in range(retry_attempts):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            print(f"Successfully connected to Kafka at {bootstrap_servers}")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1}/{retry_attempts}: Failed to connect to Kafka. Retrying in {retry_delay} seconds...")
            if attempt < retry_attempts - 1:
                time.sleep(retry_delay)
            else:
                print("Failed to connect to Kafka after all retry attempts. Exiting.")
                raise

def generate_event():
    return {
        'id': fake.uuid4(),
        'user_id': fake.random_int(min=1, max=1000),
        'event_type': fake.random_element(['click', 'view', 'purchase', 'signup']),
        'product_id': fake.random_int(min=1, max=100),
        'timestamp': datetime.now().isoformat(),
        'amount': round(fake.random.uniform(10.0, 500.0), 2),
        'country': fake.country_code(),
        'device': fake.random_element(['mobile', 'desktop', 'tablet'])
    }

def main():
    producer = create_producer()
    topic = os.getenv('KAFKA_TOPIC', 'events')
    messages_per_second = float(os.getenv('MESSAGES_PER_SECOND', '0.5'))
    sleep_interval = 1.0 / messages_per_second
    
    print(f"Starting producer for topic: {topic} at {messages_per_second} messages/second")
    
    try:
        while True:
            event = generate_event()
            try:
                producer.send(topic, value=event, key=event['id'])
                print(f"Sent: {event}")
            except Exception as e:
                print(f"Error sending message: {e}")
            time.sleep(sleep_interval)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()