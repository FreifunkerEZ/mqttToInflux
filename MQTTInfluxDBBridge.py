#!/usr/bin/env python
import re, json
from typing import NamedTuple

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

INFLUXDB_ADDRESS = '127.0.0.1'
INFLUXDB_USER = 'mqtt'
INFLUXDB_PASSWORD = 'mqtt'
INFLUXDB_DATABASE = 'tasmota'

MQTT_ADDRESS = '127.0.0.1'
MQTT_USER = ''
MQTT_PASSWORD = ''
MQTT_TOPIC = 'selfpv/tasmota/#'
MQTT_REGEX = '(.*)'
MQTT_CLIENT_ID = 'MQTTInfluxDBBridge'

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)

def flatten_dict(dd, separator='_', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten_dict(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }

class SensorData(NamedTuple):
    measurement: str
    topic: str
    values: dict

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)

def _parse_mqtt_message(topic, payload):
    """
selfpv/tasmota/STATE 
    b'{
    "Time": "2021-02-26T18:14:22",
    "Uptime": "1T19:01:32",
    "UptimeSec": 154892,
    "Heap": 26,
    "SleepMode": "Dynamic",
    "Sleep": 50,
    "LoadAvg": 24,
    "MqttCount": 1,
    "POWER": "ON",
    "Wifi": {
        "AP": 1,
        "SSId": "network S",
        "BSSId": "68:72:51:74:00:C5",
        "Channel": 11,
        "RSSI": 74,
        "Signal": -63,
        "LinkCount": 1,
        "Downtime": "0T00:00:03"
    }
}'
    
selfpv/tasmota/SENSOR 
    b'{
    "Time": "2021-02-26T18:14:22",
    "ENERGY": {
        "TotalStartTime": "2020-11-09T22:59:24",
        "Total": 3.837,
        "Yesterday": 0.164,
        "Today": 0.166,
        "Period": 0,
        "Power": 0,
        "ApparentPower": 0,
        "ReactivePower": 0,
        "Factor": 0.00,
        "Voltage": 224,
        "Current": 0.000
    }
}'

selfpv/tasmota/LWT 
    b'Online'
"""
    if topic == "selfpv/tasmota/LWT":
        values = {'status': payload.encode('utf-8')}
    else:
        values = json.loads(payload.encode('utf-8'))
    return SensorData('selfPV', topic, flatten_dict(values))


def _send_sensor_data_to_influxdb(sensor_data):
    json_body = [
        {
            'measurement': sensor_data.measurement,
            'tags': {
                'topic': sensor_data.topic
            },
            'fields': sensor_data.values
        }
    ]
    print("","","body:",json_body)
    influxdb_client.write_points(json_body)

def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.topic + ' ' + str(msg.payload))
    sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    if sensor_data is not None:
        _send_sensor_data_to_influxdb(sensor_data)

def _init_influxdb_database():
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)

def main():
    _init_influxdb_database()

    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
#    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('MQTT to InfluxDB bridge')
    main()
