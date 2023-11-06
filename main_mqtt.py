import datetime
import random
import sys
from pprint import pprint
from time import sleep
import threading
import paho.mqtt.publish as publish
import yaml
import json

global config
global producer
global topic
global SENSOR_DATA
config = dict()

SENSOR_DATA = [
    {'access_token': 'tnBwRkZACFoInUV37tf8', 'sensor_name': 'Sensor 1', 'device_id': 'e31663f0-7399-11ee-ab8c-175c8d2dbe8c'},
    {'access_token': 'lrmnYetRxWwD2PHQDvXR', 'sensor_name': 'Sensor 2', 'device_id': 'c1170350-73c0-11ee-a0ba-9579cd08332e'},
    {'access_token': '0IQDw4fdSsyQZWgs2wYb', 'sensor_name': 'Sensor 3', 'device_id': '14a0a010-76c5-11ee-8216-37552d50f785'},
    {'access_token': 'k2icVeoHUSCGQkPzzG3f', 'sensor_name': 'Sensor 4', 'device_id': '1dd8dad0-76c5-11ee-8216-37552d50f785'},
    {'access_token': 'eyNAxPUPxQxQxL3aKga5', 'sensor_name': 'Sensor 6', 'device_id': '267e7280-76c5-11ee-8216-37552d50f785'},
    {'access_token': 'UuSYWdfbIyljuaOSdbSw', 'sensor_name': 'Sensor 5', 'device_id': '222b9320-76c5-11ee-8216-37552d50f785'},
    {'access_token': 'JFhr4Ae7VXeah1RjVjKw', 'sensor_name': 'Sensor 7', 'device_id': '38148d90-76c5-11ee-8216-37552d50f785'},
    {'access_token': 'LR3NlBGmok6Zs5sMkvOI', 'sensor_name': 'Sensor 8', 'device_id': '3f2ca770-76c5-11ee-8216-37552d50f785'},
    {'access_token': 'YSqJilsrPWHTKcW7nUhm', 'sensor_name': 'Sensor 9', 'device_id': '46056e60-76c5-11ee-8216-37552d50f785'},
    {'access_token': '9s8NqvffZSfXyzoGQaRY', 'sensor_name': 'Sensor 10', 'device_id': '4d891b50-76c5-11ee-8216-37552d50f785'},
    {'access_token': 'ZbGAJG5nn0VWRVGeDiw1', 'sensor_name': 'Sensor 11', 'device_id': '419971c0-77c2-11ee-8d54-5901df55b80e'},
    {'access_token': 'xd7APknpAdOnpkpeP6kg', 'sensor_name': 'Sensor 12', 'device_id': '55278c40-77c2-11ee-8d54-5901df55b80e'},
    {'access_token': 'rvf3GRLOXihK9cNxNwD3', 'sensor_name': 'Sensor 13', 'device_id': 'b9757790-77c5-11ee-8d54-5901df55b80e'},
    {'access_token': 'eIndXHZy3IOi1DDUTFpA', 'sensor_name': 'Sensor 14', 'device_id': '22c50ac0-77c7-11ee-8d54-5901df55b80e'},
    {'access_token': 'sWX1Ld3BGllEgrrbBkwY', 'sensor_name': 'Sensor 15', 'device_id': '2b1381c0-77c7-11ee-8d54-5901df55b80e'},
    {'access_token': 's27krae8rC8ECvv1gTDp', 'sensor_name': 'Sensor 16', 'device_id': '7c59c800-77c7-11ee-8d54-5901df55b80e'},
    {'access_token': 'X1wyscCFr4xVd4xWHIsA', 'sensor_name': 'Sensor 17', 'device_id': '7eb80350-77c7-11ee-8d54-5901df55b80e'},
    {'access_token': '05wgK7gflM1lQMmPCx4K', 'sensor_name': 'Sensor 18', 'device_id': '8015e5a0-77c7-11ee-8d54-5901df55b80e'},
    {'access_token': 'y3smTGw50bt0NDXF0u2A', 'sensor_name': 'Sensor 19', 'device_id': '82b8a400-77c7-11ee-8d54-5901df55b80e'},
    {'access_token': 'eMH3vAs3iZKNIEQGfGmH', 'sensor_name': 'Sensor 20', 'device_id': '871b3ad0-77c7-11ee-8d54-5901df55b80e'},
    {'access_token': 'RMKNuAIougDAeNCbVym8', 'sensor_name': 'Sensor 21', 'device_id': '0d179d40-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'OsfcFvbXkmTnytZLGf0U', 'sensor_name': 'Sensor 22', 'device_id': '0d20ec10-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'f4Qi60HiyPqaiMUx7UrL', 'sensor_name': 'Sensor 23', 'device_id': '0d31b4f0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '1p6MRcyVCwZ9CB0RRHLt', 'sensor_name': 'Sensor 24', 'device_id': '0d3d4db0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'PVYJ6HqBpih5fFtWLAXf', 'sensor_name': 'Sensor 25', 'device_id': '0d467570-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'bKoBVWf8DceSQig2uqBy', 'sensor_name': 'Sensor 26', 'device_id': '0d520e30-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'Xm0wNNhyjcfZgDTgF3IV', 'sensor_name': 'Sensor 27', 'device_id': '0d5c2050-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'Xp3dErbSb6qE5J74KBZP', 'sensor_name': 'Sensor 28', 'device_id': '0d679200-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'hXHBd8LybK0MT4RnRjQX', 'sensor_name': 'Sensor 29', 'device_id': '0d7574b0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'qnr3yrAm6K3P4e91xqWj', 'sensor_name': 'Sensor 30', 'device_id': '0d821ee0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'qu2zocCixVDz0xz1gIsl', 'sensor_name': 'Sensor 31', 'device_id': '0d9076c0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'rKBLsgMiLdBKNGRsaDuj', 'sensor_name': 'Sensor 32', 'device_id': '0d9e0b50-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'qxdl4acJBQq9ihwglVx9', 'sensor_name': 'Sensor 33', 'device_id': '0daa1940-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'ZpvxmNYhbZneEiX5ARCp', 'sensor_name': 'Sensor 34', 'device_id': '0db7d4e0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'RukOLS9RJDEWXcFKvg68', 'sensor_name': 'Sensor 35', 'device_id': '0dc394b0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '4dUwSyPFNqg74qamSDna', 'sensor_name': 'Sensor 36', 'device_id': '0dcf7b90-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'aLOLmSTjkzl4QSYvbl3e', 'sensor_name': 'Sensor 37', 'device_id': '0dde6fb0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '0eI4VMeldsEHrMxPXE10', 'sensor_name': 'Sensor 38', 'device_id': '0deec360-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'FRE1JMy4L1IgRdOyDIwz', 'sensor_name': 'Sensor 39', 'device_id': '0dfa5c20-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'AUbgFmsgxml5V16efirQ', 'sensor_name': 'Sensor 40', 'device_id': '0e066a10-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'bWZwtqXDPW9otVNGjAjB', 'sensor_name': 'Sensor 41', 'device_id': '0e0c8490-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '69T3Jgr2CWipJhWrT7fi', 'sensor_name': 'Sensor 42', 'device_id': '0e19f210-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '3MAYboFK3mQEog1UL4wH', 'sensor_name': 'Sensor 43', 'device_id': '0e262710-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'roOnSPytUEVg4qTaYAsJ', 'sensor_name': 'Sensor 44', 'device_id': '0e3409c0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'BFc4gxEWfRxILTonvMdY', 'sensor_name': 'Sensor 45', 'device_id': '0e3d7fa0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'bAcfip6e9VUmSL2s4xdB', 'sensor_name': 'Sensor 46', 'device_id': '0e4bd780-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'VFDE6pEX0XaTZ78oNZBY', 'sensor_name': 'Sensor 47', 'device_id': '0e57be60-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'aFMICEJDw6IyxWmFfV2q', 'sensor_name': 'Sensor 48', 'device_id': '0e610d30-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'QjDexcZV2LuRjt3SuYG0', 'sensor_name': 'Sensor 49', 'device_id': '0e7160e0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'grNpD7TdIdyTtH4b0VBA', 'sensor_name': 'Sensor 50', 'device_id': '0e7c8470-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'mCWqWWD9TqGh2WdgzMDc', 'sensor_name': 'Sensor 51', 'device_id': '0e86e4b0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'lPbdk3MPelWzFoNR6XMD', 'sensor_name': 'Sensor 52', 'device_id': '0e9563a0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'cF1nhXUPtOxZvtx1sSpx', 'sensor_name': 'Sensor 53', 'device_id': '0ea36d60-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'k6BakQZNQC7v0bG3JiXS', 'sensor_name': 'Sensor 54', 'device_id': '0ead0a50-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'n4szPtQ2HdZ9qXO46WNl', 'sensor_name': 'Sensor 55', 'device_id': '0eb9db90-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '9kMszmw7VBwkO8yPz4Wj', 'sensor_name': 'Sensor 56', 'device_id': '0ec37880-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'YASakJPwAELTq72fyL7I', 'sensor_name': 'Sensor 57', 'device_id': '0ed0e600-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'thnDIyPW2nKBnMOQGktC', 'sensor_name': 'Sensor 58', 'device_id': '0edc7ec0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'LXpNnwOsFuxbFSDECbQE', 'sensor_name': 'Sensor 59', 'device_id': '0eead6a0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'BErLcpDs3onYb84Nt78z', 'sensor_name': 'Sensor 60', 'device_id': '0ef4c1b0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'SMSu1eJjgrxbWQsPi1yb', 'sensor_name': 'Sensor 61', 'device_id': '0f00cfa0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '4xV8aEGBKkB7qVSgQjvk', 'sensor_name': 'Sensor 62', 'device_id': '0f073840-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'NKgz42ef4nrmbzDLgWTb', 'sensor_name': 'Sensor 63', 'device_id': '0f15b730-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'AN8sGQuA6IzznTe23J3d', 'sensor_name': 'Sensor 64', 'device_id': '0f21c520-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'leNuWy367JaPRYTayDg6', 'sensor_name': 'Sensor 65', 'device_id': '0f31f1c0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'UwYkycGJ8z9q9bpH4FuA', 'sensor_name': 'Sensor 66', 'device_id': '0f429390-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'V1PCHECeHzDlzkTYA7tP', 'sensor_name': 'Sensor 67', 'device_id': '0f50c460-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'tEUudEHQMhnoaJwXSqRM', 'sensor_name': 'Sensor 68', 'device_id': '0f5cab40-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'E8UDFTRfl6XhfPqHTRr5', 'sensor_name': 'Sensor 69', 'device_id': '0f65d300-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'v2jIFeGOBE4u74HC6RKQ', 'sensor_name': 'Sensor 70', 'device_id': '0f7403d0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'jYpexxJrTdFra644AcE7', 'sensor_name': 'Sensor 71', 'device_id': '0f7feab0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'fM2Top4yYabgmq8qgptN', 'sensor_name': 'Sensor 72', 'device_id': '0f8e4290-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'gQHbAvT9YX4C9E5t6MJv', 'sensor_name': 'Sensor 73', 'device_id': '0f9cc180-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'aCyD3j8sGIugpdfIvnaL', 'sensor_name': 'Sensor 74', 'device_id': '0facee20-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '7HT4E1dY3kKvunRGWcTD', 'sensor_name': 'Sensor 75', 'device_id': '0fb8adf0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'mL2SIAhksfpBj9HNcKZw', 'sensor_name': 'Sensor 76', 'device_id': '0fc6dec0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'v0AxzNaPZyLjAU00EozP', 'sensor_name': 'Sensor 77', 'device_id': '0fd536a0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'svvrWgx2oSfS7Jby1qhW', 'sensor_name': 'Sensor 78', 'device_id': '0fe34060-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'pt6fB6OhRvr4tH6DQD9u', 'sensor_name': 'Sensor 79', 'device_id': '0fef9c70-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'AxudyeP0L835cKku5DXL', 'sensor_name': 'Sensor 80', 'device_id': '0ffd7f20-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'VL81RqjK0Y1vabTO5EkE', 'sensor_name': 'Sensor 81', 'device_id': '100bfe10-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '3JzeXfrUthauBkwfmpZS', 'sensor_name': 'Sensor 82', 'device_id': '101a07d0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'wqjJ76suucNjszi6LwVZ', 'sensor_name': 'Sensor 83', 'device_id': '102615c0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'luKnxy2B8CewwyXmFzo1', 'sensor_name': 'Sensor 84', 'device_id': '1031ae80-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '5TgOge2zcD15nycKywAq', 'sensor_name': 'Sensor 85', 'device_id': '103b4b70-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'Atf5kLgxwJGO8ZEdZNet', 'sensor_name': 'Sensor 86', 'device_id': '10470b40-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'cTOajImty6dH62xJjVa0', 'sensor_name': 'Sensor 87', 'device_id': '10556320-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'nSGQzWCjVHMNcN9foZhv', 'sensor_name': 'Sensor 88', 'device_id': '105d0440-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '9GtgQJ5hHH6L5vmTybvG', 'sensor_name': 'Sensor 89', 'device_id': '106abfe0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'dyvsqqojr1mERXE5mLxw', 'sensor_name': 'Sensor 90', 'device_id': '1076a6c0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '1SpEPSery9nSeJCB1Uyy', 'sensor_name': 'Sensor 91', 'device_id': '107daba0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'iBSxaJoDJxRUtEtxb4ob', 'sensor_name': 'Sensor 92', 'device_id': '108dff50-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '8db9XGHDwyXQple2foMw', 'sensor_name': 'Sensor 93', 'device_id': '109c7e40-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'wx4m8GmCH5SkrExVLh74', 'sensor_name': 'Sensor 94', 'device_id': '10a81700-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '7JqHi1VYzOXISNTuJrDv', 'sensor_name': 'Sensor 95', 'device_id': '10b424f0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '6n3mftYFYjV4akm1Hhd4', 'sensor_name': 'Sensor 96', 'device_id': '10bfe4c0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'oUa0Dr0VFhzi1lpgpzQS', 'sensor_name': 'Sensor 97', 'device_id': '10ce63b0-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'o0iBf5OQ3pwDMFrHrSPP', 'sensor_name': 'Sensor 98', 'device_id': '10da4a90-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': 'sy6Yq6lq5a5YKEYtPM5B', 'sensor_name': 'Sensor 99', 'device_id': '2fbca200-77c8-11ee-8d54-5901df55b80e'},
    {'access_token': '48FRn8NiiN0JrjFAeizd', 'sensor_name': 'Sensor 100', 'device_id': '32679dc0-77c8-11ee-8d54-5901df55b80e'}
]

threads = []


def main(argv):
    if len(argv) < 1:
        print("INVALID EXECUTION! sensor count must be need!")
        return
    global SENSOR_DATA
    global threads
    SENSOR_COUNT = argv[1]
    for i in range(0, int(SENSOR_COUNT)):
        SENSOR = SENSOR_DATA[i]
        ACCESS_TOKEN = SENSOR['access_token']
        DEVICE_ID = SENSOR['device_id']
        SENSOR_NAME = SENSOR['sensor_name']
        thread = threading.Thread(target=job, args=(i, ACCESS_TOKEN, SENSOR_NAME, DEVICE_ID))
        threads.append(thread)
        thread.start()


def job(thread_num, access_token, sensor_name, device_id):
    while True:
        mock_msg = mock_parameters(access_token, sensor_name, device_id)
        pprint(mock_msg)
        send_mqtt_to_framework(access_token, mock_msg)
        sleep(1)


def read_config():
    global config
    with open('config.yaml', 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)


def mock_parameters(access_token, sensor_name, device_id):
    return_parameter = dict()
    return_parameter['assetId'] = access_token
    return_parameter['sensorName'] = sensor_name
    return_parameter['deviceId'] = device_id
    return_parameter['param_1'] = random.random() * 100
    return_parameter['param_2'] = random.random() * 40
    for i in range(3, 101):
        return_parameter[f'param_{i}'] = random.random() * 30
    return_parameter['timestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    return return_parameter


def send_mqtt_to_framework(access_token, message):
    global config
    mqtt_config = config.get('mqtt')
    send_mqtt_message(mqtt_config.get('host'), mqtt_config.get('port'), mqtt_config.get('topic'), access_token, message)


def send_mqtt_message(hostname, port, topic, username, message):
    publish.single(topic, payload=json.dumps(message), hostname=hostname, port=port, auth={'username': username}, qos=0)


if __name__ == '__main__':
    read_config()
    main(sys.argv)
