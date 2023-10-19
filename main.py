import datetime
import random
import sys
from time import sleep
from json import dumps
from pprint import pprint
import yaml

from kafka import KafkaProducer

global config
global producer
global topic

config = dict()
producer = None
topic = 'datapipeline.test.sensor'


def read_config():
    global config
    with open('config.yaml', 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)


def init_producer():
    global config
    global producer
    pprint(config)
    producer = KafkaProducer(bootstrap_servers=config['bootstrap_servers'],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))


def main(argv):
    global producer
    global topic
    if len(argv) < 2:
        print("INVALID EXECUTION! Sensor name must be needed!")
        return
    if len(argv) == 3:
        topic = argv[2]
    SENSOR_NAME = argv[1]
    if SENSOR_NAME is None:
        print("SENSOR NAME CANNOT BE EMPTY! FILL IN FIRST CMD PARAMETER")
        return
    while True:
        mock_msg = mock_parameters(SENSOR_NAME)
        pprint(mock_msg)
        producer.send(topic, mock_msg)
        sleep(1)


def mock_parameters(sensor_name):
    return_parameter = dict()
    return_parameter['sensor_name'] = sensor_name
    return_parameter['param_1'] = random.random() * 100
    return_parameter['param_2'] = random.random() * 40
    return_parameter['param_3'] = random.random() * 30
    return_parameter['timestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    return return_parameter


if __name__ == '__main__':
    read_config()
    init_producer()
    main(sys.argv)
