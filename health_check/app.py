import requests
import logging
import logging.config
import yaml
import json
import datetime
import os
import connexion
from connexion import NoContent
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


def check_service_health(service_name, url):
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            logger.info(f"Successfully retrieved health status of {service_name}")
            return 'Running'
    except requests.RequestException as e:
        logger.error(f"Failed to connect to {service_name}: {e}")
    return 'Down'

def update_health_status():
    logger.info("Started retrieving health status of all the services.")        
    current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    health_status = {
        'receiver': check_service_health('Receiver', app_config['eventstore']['url'] + app_config['service']['receiver']),
        'storage': check_service_health('Storage', app_config['eventstore']['url'] + app_config['service']['storage']),
        'processing': check_service_health('Processing', app_config['eventstore']['url'] + app_config['service']['processing']),
        'audit': check_service_health('Audit', app_config['eventstore']['url'] + app_config['service']['audit']),
        'last_update': current_datetime
    }
    
    with open(app_config['datastore']['filename'], 'w') as file:
        json.dump(health_status, file, indent=2)

    logger.info("Retrieved health status of all the services.")      
    return health_status, 200  


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(update_health_status, 'interval', seconds=app_config['scheduler']['period_sec'])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml",
            base_path="/health_check",
            strict_validation=True,
            validate_responses=True)

if __name__ == "__main__":
    if "TARGET_ENV" not in os.environ or os.environ["TARGET_ENV"] != "test":
        CORS(app.app)
        app.app.config['CORS_HEADERS'] = 'Content-Type'
    # run our standalone gevent server
    init_scheduler()
    app.run(port=8120, use_reloader=False)