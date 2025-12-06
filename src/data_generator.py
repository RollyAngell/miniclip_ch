import json
import time
import random
import os
from kafka import KafkaProducer
from faker import Faker

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME = 'events-raw'
SLEEP_INTERVAL = 0.5  # Seconds between events

# Initialize Faker
fake = Faker()

# Platform and Country options (for consistency)
PLATFORMS = ['android', 'ios', 'web', 'facebook']
COUNTRIES = ['US', 'PT', 'GB', 'BR', 'FR', 'DE', 'IT', 'ES']

# In-memory user database to simulate active sessions and consistent user data
# Format: {user_id: {'country': 'US', 'platform': 'ios', 'coins': 1000, 'level': 5}}
active_users = {}

def get_producer():
    """Creates and returns a KafkaProducer instance."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
            time.sleep(2)

def generate_user():
    """Generates a new user and adds it to the active pool."""
    user_id = str(random.randint(10000, 999999))
    country = random.choice(COUNTRIES)
    platform = random.choice(PLATFORMS)
    
    user_data = {
        'country': country,
        'platform': platform,
        'coins': random.randint(100, 5000),
        'level': random.randint(1, 50),
        'device': f"{platform}-device-{random.randint(1, 9)}"
    }
    active_users[user_id] = user_data
    return user_id, user_data

def create_init_event():
    """Creates an 'Init' event."""
    user_id, user_data = generate_user()
    
    event = {
        "event-type": "init",
        "time": int(time.time()),
        "user-id": user_id,
        "country": user_data['country'],
        "platform": user_data['platform']
    }
    return event

def create_match_event():
    """Creates a 'Match' event between two existing users."""
    if len(active_users) < 2:
        return None
    
    # Select two different users
    user_a_id, user_b_id = random.sample(list(active_users.keys()), 2)
    user_a = active_users[user_a_id]
    user_b = active_users[user_b_id]
    
    # Simulate match results (updates state)
    user_a['coins'] += random.choice([-100, 100]) # Win or lose coins
    user_b['coins'] += random.choice([-100, 100])
    user_a['level'] += random.choice([0, 1]) # Level up or stay
    user_b['level'] += random.choice([0, 1])
    
    winner = random.choice([user_a_id, user_b_id])
    
    event = {
        "event-type": "match",
        "time": int(time.time()),
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
        "winner": winner,
        "game-tier": random.randint(1, 10),
        "duration": random.randint(60, 600)
    }
    return event

def create_purchase_event():
    """Creates an 'In-App Purchase' event."""
    if not active_users:
        return None
        
    user_id = random.choice(list(active_users.keys()))
    
    event = {
        "event-type": "in-app-purchase",
        "time": int(time.time()),
        "purchase_value": round(random.uniform(0.99, 99.99), 2),
        "user-id": user_id,
        "product-id": f"prod_{random.randint(1, 20)}"
    }
    return event

def main():
    producer = get_producer()
    print("Starting event generation...")
    
    # Pre-populate some users
    for _ in range(10):
        create_init_event()

    while True:
        # Probability distribution for event types
        rand_val = random.random()
        
        event = None
        if rand_val < 0.15:
            event = create_init_event()
        elif rand_val < 0.40:
            event = create_purchase_event()
        else:
            event = create_match_event()
            
        if event:
            print(f"Sending {event['event-type']} event")
            producer.send(TOPIC_NAME, event)
            
        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    main()

