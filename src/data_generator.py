import json
import time
import random
import os
import logging
import sys
from kafka import KafkaProducer
from faker import Faker

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
class Config:
    KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    TOPIC_NAME = 'events-raw'
    MIN_SLEEP = 0.01
    MAX_SLEEP = 0.1
    BURST_MIN = 1
    BURST_MAX = 5

# Initialize Faker
fake = Faker()

# Reference Data
PLATFORMS = ['android', 'ios', 'web', 'facebook', 'xbox', 'ps5', 'switch']
COUNTRIES = ['US', 'PT', 'GB', 'BR', 'FR', 'DE', 'IT', 'ES', 'CA', 'AU', 'JP', 'KR', 'IN']

# In-memory user database
active_users = {}

def get_producer() -> KafkaProducer:
    """Creates and returns a KafkaProducer instance with retry logic."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=Config.KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Connected to Kafka at {Config.KAFKA_BOOTSTRAP}")
            return producer
        except Exception as e:
            logger.warning(f"Waiting for Kafka... ({e})")
            time.sleep(2)

def generate_user() -> tuple:
    """Generates a new user and adds it to the active pool."""
    user_id = str(random.randint(10000, 9999999))
    user_data = {
        'country': random.choice(COUNTRIES),
        'platform': random.choice(PLATFORMS),
        'coins': random.randint(100, 50000),
        'level': random.randint(1, 100),
        'device': f"{random.choice(PLATFORMS)}-device-{random.randint(1, 99)}"
    }
    active_users[user_id] = user_data
    return user_id, user_data

def create_init_event() -> dict:
    """Creates an 'Init' event."""
    user_id, user_data = generate_user()
    return {
        "event-type": "init",
        "time": int(time.time()),
        "user-id": user_id,
        "country": user_data['country'],
        "platform": user_data['platform']
    }

def create_match_event() -> dict:
    """Creates a 'Match' event between two existing users."""
    if len(active_users) < 2:
        return None
    
    user_a_id, user_b_id = random.sample(list(active_users.keys()), 2)
    user_a = active_users[user_a_id]
    user_b = active_users[user_b_id]
    
    # Simulate state changes
    user_a['coins'] += random.choice([-500, -100, 100, 500, 1000])
    user_b['coins'] += random.choice([-500, -100, 100, 500, 1000])
    user_a['level'] += random.choice([0, 0, 1, 2])
    user_b['level'] += random.choice([0, 0, 1, 2])
    
    return {
        "event-type": "match",
        "time": int(time.time()),
        "country": user_a['country'],
        "user-a": user_a_id,
        "user-b": user_b_id,
        "user-a-postmatch-info": {
            "coin-balance-after-match": max(0, user_a['coins']),
            "level-after-match": user_a['level'],
            "device": user_a['device'],
            "platform": user_a['platform']
        },
        "user-b-postmatch-info": {
            "coin-balance-after-match": max(0, user_b['coins']),
            "level-after-match": user_b['level'],
            "device": user_b['device'],
            "platform": user_b['platform']
        },
        "winner": random.choice([user_a_id, user_b_id]),
        "game-tier": random.randint(1, 20),
        "duration": random.randint(30, 900)
    }

def create_purchase_event() -> dict:
    """Creates an 'In-App Purchase' event."""
    if not active_users:
        return None
        
    user_id = random.choice(list(active_users.keys()))
    user_data = active_users[user_id]
    
    return {
        "event-type": "in-app-purchase",
        "time": int(time.time()),
        "purchase_value": round(random.uniform(0.99, 199.99), 2),
        "user-id": user_id,
        "country": user_data['country'],
        "product-id": f"prod_{random.randint(1, 100)}"
    }

def main():
    producer = get_producer()
    logger.info("Starting event generation...")
    
    # Pre-populate users
    logger.info("Pre-populating 50 users...")
    for _ in range(50):
        create_init_event()

    try:
        while True:
            burst_size = random.randint(Config.BURST_MIN, Config.BURST_MAX)
            
            for _ in range(burst_size):
                rand_val = random.random()
                event = None
                
                if rand_val < 0.20:
                    event = create_init_event()
                elif rand_val < 0.45:
                    event = create_purchase_event()
                else:
                    event = create_match_event()
                    
                if event:
                    logger.info(f"Sending {event['event-type']} event")
                    producer.send(Config.TOPIC_NAME, event)
                
            time.sleep(random.uniform(Config.MIN_SLEEP, Config.MAX_SLEEP))
            
    except KeyboardInterrupt:
        logger.info("Stopping event generation...")
        producer.close()

if __name__ == "__main__":
    main()
