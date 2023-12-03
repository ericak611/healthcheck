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

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

# External Logging Configuration
with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())

logging.config.dictConfig(log_config)
logger = logging.getLogger('basicLogger')
logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)

def get_health_status():
    logger.info("service is running")
    return 200

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


def add_book_hold(body): 
    trace_id = str(uuid.uuid4())
    message = f"Receievd event book hold request with trace_id: {trace_id}"
    logger.info(message)

    event_object = {
        "trace_id": trace_id,
        **body
    }

    msg = { "type": "book",
            "datetime" :
                datetime.datetime.now().strftime(
                    "%Y-%m-%dT%H:%M:%S"),
            "payload": event_object }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    logger.info("Sending the book hold request to Kafka")
       

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

    msg = { "type": "movie",
            "datetime" :
                datetime.datetime.now().strftime(
                    "%Y-%m-%dT%H:%M:%S"),
            "payload": event_object }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    logger.info("Sending the movie hold request to Kafka")
    # logger.info(f"Returned event movie hold request with response {trace_id} with status {response.status_code}")      
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml",
            base_path="/receiver",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080)

