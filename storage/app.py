import connexion
from connexion import NoContent

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from book_hold import BookHold
from movie_hold import MovieHold
import datetime
import yaml
import logging 
import logging.config
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
import json
from sqlalchemy import and_
import time 

import mysql.connector
import pymysql
import os

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

user = app_config['datastore']['user']
password = app_config['datastore']['password']
hostname = app_config['datastore']['hostname']
port = app_config['datastore']['port']
db = app_config['datastore']['db']


DB_ENGINE = create_engine(f'mysql+pymysql://{user}:{password}@{hostname}:{port}/{db}')
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)



def get_book_hold(start_timestamp, end_timestamp):
    """ Gets new book hold requests after the timestamp """
    message1 = f"Connecting to DB. Hostname: {app_config['datastore']['hostname']}, Port: {app_config['datastore']['port']}"
    logger.info(message1)
    session = DB_SESSION()
    
    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S.%f')
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S.%f')

    book_hold_requests = session.query(BookHold).filter(and_(BookHold.date_created >= start_timestamp_datetime, 
                                                             BookHold.date_created < end_timestamp_datetime))
    book_requests_list = []

    for hold in book_hold_requests:
        book_requests_list.append(hold.to_dict())
    
    session.close()

    logger.info("Query for book hold requests after %s returns %d results" % (start_timestamp, len(book_requests_list)))
    
    # logger.info(f"Connecting to DB. Hostname: {app_config['datastore']['hostname']}, Port: app_config['datastore']['port']")    
    print(book_requests_list)
    return book_requests_list, 200

def get_movie_hold(start_timestamp, end_timestamp):
    """ Gets new movie hold requests after the timestamp """
    message1 = f"Connecting to DB. Hostname: {app_config['datastore']['hostname']}, Port: {app_config['datastore']['port']}"
    logger.info(message1)
    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S.%f')
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S.%f')
    
    movie_hold_requests = session.query(MovieHold).filter(and_(MovieHold.date_created >= start_timestamp_datetime, 
                                                               MovieHold.date_created < end_timestamp_datetime))    
    
    movie_requests_list = []

    for hold in movie_hold_requests:
        movie_requests_list.append(hold.to_dict())
    
    session.close()

    # logger.info("Query for movie hold requests after %s returns %d results" % (timestamp, len(movie_requests_list)))
    
    print(movie_requests_list)
    return movie_requests_list, 200

def process_messages():
    """ Process event messages """

    hostname = "%s:%d" % (app_config["events"]["hostname"],
                          app_config["events"]["port"])
    
    retry_count = 0
    max_retries = app_config["kafka"]["max_retries"]
    while retry_count < max_retries:
        try:            
            logger.info(f"Trying to connect to Kafka. Attempt #{retry_count + 1}")
            client = KafkaClient(hosts=hostname)
            topic = client.topics[str.encode(app_config["events"]["topic"])] 
            logger.info(f"Successfully connected to Kafka.")
            break
        except Exception as e:
            logger.info(f"Failed to connect to Kafka. Error: {str(e)}")
            sleep_time = app_config["kafka"]["sleep_seconds"]
            logger.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)

            retry_count += 1

    if retry_count == max_retries:
        logger.info(f"Failed to connect to Kafka after {max_retries} attempts.")   
                

    consumer = topic.get_simple_consumer(consumer_group=b'event_group',
                                         reset_offset_on_start=False,
                                         auto_offset_reset=OffsetType.LATEST)
    
    # This is blocking - it will wait for a new message
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)

        payload = msg["payload"] 

        if msg["type"] == "book": # Change this to your event type

            message1 = f"Connecting to DB. Hostname: {app_config['datastore']['hostname']}, Port: {app_config['datastore']['port']}"
            logger.info(message1)
        # Store the event1 (i.e., the payload) to the DB
            session = DB_SESSION()
            bh = BookHold(payload['book_id'],
                          payload['user_id'],
                          payload['branch_id'],
                          payload['availability'],
                          payload['timestamp'],
                          payload['trace_id'])
    
            session.add(bh)

            session.commit()
            session.close()

        elif msg["type"] == "movie": # Change this to your event type
            message1 = f"Connecting to DB. Hostname: {app_config['datastore']['hostname']}, Port: {app_config['datastore']['port']}"
            logger.info(message1)
        # Store the event2 (i.e., the payload) to the DB
            session = DB_SESSION()
            mh = MovieHold(payload['movie_id'],
                          payload['user_id'],
                          payload['branch_id'],
                          payload['availability'],
                          payload['timestamp'],
                          payload['trace_id'])
    
            session.add(mh)

            session.commit()
            session.close()
        # Commit the new message as being read
        consumer.commit_offsets()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml",
            base_path="/storage",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090)

