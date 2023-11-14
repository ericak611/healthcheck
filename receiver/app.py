import logging 
import logging.config
import yaml 
import json 
import datetime
import os 
import connexion
from connexion import NoContent 
import requests
import random
import uuid
import pykafka 
from pykafka import KafkaClient
import time

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())
    
with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

# client = KafkaClient(hosts='<kafka-server>:<kafka-port>')
# f"{app_config['events']['hostname']}:{app_config['events']['port']}"
# topic = client.topics[str.encode(app_config['events']['topic'])]
# producer = topic.get_sync_producer() 

def connect_to_kafka(hostname, app_config):
    retry_count = 0
    max_retries = app_config["kafka"]["max_retries"]

    while retry_count < max_retries:
        try:
            logger.info(f"Trying to connect to Kafka. Attempt #{retry_count + 1}")
            client = KafkaClient(hosts=f"{hostname}")
            topic = client.topics[str.encode(app_config["events"]["topic"])]
            producer = topic.get_sync_producer()
            logger.info(f"Successfully connected to Kafka.")
            return producer
        except Exception as e:
            logger.error(f"Connection to Kafka failed. Error: {str(e)}")
            sleep_time = app_config["kafka"]["sleep_seconds"]
            logger.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)

            retry_count += 1

    logger.error(f"Failed to connect to Kafka after {max_retries} attempts.")
    return None, None

# Create KafkaClient when Receiver Service starts up
producer = connect_to_kafka(
    f"{app_config['events']['hostname']}:{app_config['events']['port']}",
    app_config)

# def get_kafka_producer():
#     client, topic = connect_to_kafka(
#     f"{app_config['events']['hostname']}:{app_config['events']['port']}",
#     app_config)
#     return topic.get_sync_producer()

def add_book_hold(body): 
    trace_id = str(uuid.uuid4())
    message = f"Receievd event book hold request with trace_id: {trace_id}"
    logger.info(message)

    event_object = {
        "trace_id": trace_id,
        **body
    }
    
    try:
        msg = { "type": "book",
                "datetime" :
                    datetime.datetime.now().strftime(
                        "%Y-%m-%dT%H:%M:%S"),
                "payload": event_object }
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        logger.info("Sending the book hold request to Kafka")
    except:
        logger.error(f"Producer stopped. Attempting to reconnect to Kafka.")

        msg = {"type": "book",
               "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
               "payload": event_object}
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        logger.info("Sent the book hold request to Kafka after reconnect")        

    # logger.info(f"Returned event movie hold request with response {trace_id} with status {response.status_code}")
    return NoContent, 201


def add_movie_hold(body):
    trace_id = str(uuid.uuid4())
    message = f"Receievd event movie hold request with trace_id: {trace_id}"
    logger.info(message)


    event_object = {
        "trace_id": trace_id,
        **body
    }

    try:
        msg = { "type": "movie",
                "datetime" :
                    datetime.datetime.now().strftime(
                        "%Y-%m-%dT%H:%M:%S"),
                "payload": event_object }
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        logger.info("Sending the movie hold request to Kafka")
    # logger.info(f"Returned event movie hold request with response {trace_id} with status {response.status_code}")
    except:
        logger.error(f"Producer stopped. Attempting to reconnect to Kafka.")
        
        msg = { "type": "movie",
                "datetime" :
                    datetime.datetime.now().strftime(
                        "%Y-%m-%dT%H:%M:%S"),
                "payload": event_object }
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        logger.info("Sending the moive hold request to Kafka")        
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080)

