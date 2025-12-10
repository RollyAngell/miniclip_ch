import json
import os
import sys
import logging
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, Any

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
    SOURCE_TOPIC = 'events-raw'
    DESTINATION_TOPIC = 'events-enriched'
    GROUP_ID = 'data-quality-group'

# Reference Data
COUNTRY_MAPPING = {
    'US': 'United States', 'PT': 'Portugal', 'GB': 'United Kingdom',
    'BR': 'Brazil', 'FR': 'France', 'DE': 'Germany',
    'IT': 'Italy', 'ES': 'Spain'
}

def get_consumer() -> KafkaConsumer:
    """Creates and returns a KafkaConsumer instance."""
    return KafkaConsumer(
        Config.SOURCE_TOPIC,
        bootstrap_servers=Config.KAFKA_BOOTSTRAP,
        group_id=Config.GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest' 
    )

def get_producer() -> KafkaProducer:
    """Creates and returns a KafkaProducer instance."""
    return KafkaProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def transform_platform(data: Dict[str, Any], key: str = 'platform'):
    """Helper to uppercase platform field if exists."""
    if key in data and isinstance(data[key], str):
        data[key] = data[key].upper()

def transform_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Applies data quality transformations to a single event."""
    transformed_event = event.copy()
    
    # 1. Transform Platform (Root and Nested)
    transform_platform(transformed_event)
    
    for nested_key in ['user-a-postmatch-info', 'user-b-postmatch-info']:
        if nested_key in transformed_event:
            transform_platform(transformed_event[nested_key])

    # 2. Map Country Code
    if 'country' in transformed_event:
        code = transformed_event['country']
        transformed_event['country'] = COUNTRY_MAPPING.get(code, code)

    return transformed_event

def main():
    logger.info("Starting Data Quality Service...")
    logger.info(f"Consuming from: {Config.SOURCE_TOPIC}")
    logger.info(f"Producing to: {Config.DESTINATION_TOPIC}")

    consumer = None
    producer = None

    try:
        consumer = get_consumer()
        producer = get_producer()
        
        logger.info("Service is running...")

        for message in consumer:
            try:
                enriched_event = transform_event(message.value)
                producer.send(Config.DESTINATION_TOPIC, enriched_event)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Stopping Data Quality Service...")
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
    finally:
        if producer: producer.close()
        if consumer: consumer.close()

if __name__ == "__main__":
    main()
