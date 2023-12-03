import requests
import logging 
import logging.config
import yaml 
import json 
import datetime
import os 
import connexion
from connexion import NoContent 
import random
import uuid
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS, cross_origin


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

def populate_stats():

    # Log an INFO message 
    logger.info("Start periodic processing.")

    hold_requests = {
    'num_bh_requests': 0,
    'num_mh_requests': 0,
    'max_bh_availability': 0,
    'max_mh_availability': 0,
    'last_updated': '2010-10-10 11:17:50.225086'
    }

    if os.path.exists(app_config['datastore']['filename']) :
        with open(app_config['datastore']['filename'], 'r') as file:
            hold_requests = json.load(file)

    current_datetime = datetime.datetime.now()
    timestamp_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')

    book_response = requests.get(
        app_config['eventstore']['url']+"/book",
        params={"start_timestamp": hold_requests['last_updated'],
                "end_timestamp": timestamp_datetime}
    )

    movie_response = requests.get(
        app_config['eventstore']['url']+"/movie",
        params={"start_timestamp": hold_requests['last_updated'],
                "end_timestamp": timestamp_datetime}
    )

    # Log based on status code 
    if book_response.status_code == 200:
        new_book_requests = book_response.json()
        num_book_requests = len(new_book_requests) 
        if num_book_requests > 0:
            hold_requests['num_bh_requests'] += num_book_requests

        logger.info(f"Received {len(new_book_requests)} events from /book")
    else:
        logger.error("Falied to get events from /book")
    
    # Log based on status code 
    if movie_response.status_code == 200:
        new_movie_requests = movie_response.json()
        num_movie_requests = len(new_movie_requests) 
        if num_movie_requests > 0:
            hold_requests['num_mh_requests'] += num_movie_requests

        logger.info(f"Received {len(new_book_requests)} events from /movie")
    else:
        logger.error("Falied to get events from /movie")
        

    current_book_max = hold_requests['max_bh_availability']  
    current_movie_max = hold_requests['max_mh_availability']  

    new_book_max = max([d["availability"] for d in new_book_requests], default=0)
    new_movie_max = max([d["availability"] for d in new_movie_requests], default=0)

    # Update max availability with the new maximum values
    hold_requests['max_bh_availability'] = new_book_max + current_book_max
    hold_requests['max_mh_availability'] = new_movie_max + current_movie_max

    # current_datetime = datetime.datetime.now()
    # timestamp_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')

    hold_requests['last_updated'] = timestamp_datetime

    with open(app_config['datastore']['filename'], 'w') as file:
        json.dump(hold_requests, file, indent=2) 
    # return hold_requests, 200

def get_stats(): 
    logger.info("Request for statistics has started")

    if not os.path.exists(app_config['datastore']['filename']) :
        logger.error("Statistics do not exist!")
        return f"Statistics do not exist", 404 

    # i get a list of dictionary of events
    if os.path.exists(app_config['datastore']['filename']) :
        with open(app_config['datastore']['filename'], 'r') as file: 
            current_stats = json.load(file)   

        logger.debug(current_stats)
        logger.info("Request has completed!")

        return current_stats, 200

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
    'interval',
    seconds=app_config['scheduler']['period_sec'])
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml",
            base_path="/processing",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    if "TARGET_ENV" not in os.environ or os.environ["TARGET_ENV"] != "test":
        CORS(app.app)
        app.app.config['CORS_HEADERS'] = 'Content-Type'
    # run our standalone gevent server
    init_scheduler()
    app.run(port=8100, use_reloader=False)
