import json
import os
import logging
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, Any, Optional

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
SOURCE_TOPIC = 'events-raw'
DESTINATION_TOPIC = 'events-enriched'
GROUP_ID = 'data-quality-group'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Transformation Rules (Reference Data) ---
COUNTRY_MAPPING = {
    'US': 'United States',
    'PT': 'Portugal',
    'GB': 'United Kingdom',
    'BR': 'Brazil',
    'FR': 'France',
    'DE': 'Germany',
    'IT': 'Italy',
    'ES': 'Spain'
}

def get_consumer() -> KafkaConsumer:
    """Creates and returns a KafkaConsumer instance."""
    return KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest' 
    )

def get_producer() -> KafkaProducer:
    """Creates and returns a KafkaProducer instance."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def transform_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Applies data quality transformations to a single event.
    
    Transformations:
    1. Uppercase 'platform' field.
    2. Map 'country' code to full country name.
    """
    # Create a copy to avoid mutating the original dictionary if passed by reference elsewhere
    transformed_event = event.copy()
    
    # --- Transformation 1: Uppercase Platform ---
    # Platform can exist at the root level (Init) or inside nested objects (Match)
    
    if 'platform' in transformed_event:
        transformed_event['platform'] = transformed_event['platform'].upper()
        
    # Check nested structures in 'match' events
    if 'user-a-postmatch-info' in transformed_event and 'platform' in transformed_event['user-a-postmatch-info']:
        transformed_event['user-a-postmatch-info']['platform'] = \
            transformed_event['user-a-postmatch-info']['platform'].upper()
            
    if 'user-b-postmatch-info' in transformed_event and 'platform' in transformed_event['user-b-postmatch-info']:
        transformed_event['user-b-postmatch-info']['platform'] = \
            transformed_event['user-b-postmatch-info']['platform'].upper()

    # --- Transformation 2: Country Code Mapping ---
    if 'country' in transformed_event:
        original_country = transformed_event['country']
        # Default to original if not found in mapping
        transformed_event['country'] = COUNTRY_MAPPING.get(original_country, original_country)

    return transformed_event

def main():
    logger.info("Starting Data Quality Service...")
    logger.info(f"Consuming from: {SOURCE_TOPIC}")
    logger.info(f"Producing to: {DESTINATION_TOPIC}")

    try:
        consumer = get_consumer()
        producer = get_producer()
        
        logger.info("Service is running and waiting for events...")

        for message in consumer:
            try:
                raw_event = message.value
                
                # Apply transformations
                enriched_event = transform_event(raw_event)
                
                # Send to destination topic
                producer.send(DESTINATION_TOPIC, enriched_event)
                
                # Optional: log periodically or on debug to avoid flooding logs
                # logger.debug(f"Processed event: {enriched_event.get('event-type')}")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Stopping Data Quality Service...")
    except Exception as e:
        logger.critical(f"Critical error: {e}")
    finally:
        if 'producer' in locals():
            producer.close()
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    main()

