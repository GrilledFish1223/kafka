#!/usr/bin/env python
# encoding: utf-8

"""
@author: zsp
@file: classTest.py.py
@time: 2019-04-12 21:45
@desc:
"""
import sys
import time
import json

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_HOST = "127.0.0.1"
KAFKA_PORT = 9092
KAFKA_TOPIC = "test"


class Kafka_Producer():
    def __init__(self, kafkahost, kafkaport, kafkatopic, key):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkaTopic = kafkatopic
        self.key = key

        bootstrap_servers = '{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort
        )
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def sendMessage(self, param):
        try:
            param_msg = json.dumps(param, encoding=False)
            producer = self.producer
            producer.send(self.kafkaTopic, key=self.key, varlue=param_msg.encode('utf-8'))
            producer.flush()
        except KafkaProducer as e:
            print e


class Kafka_consumer():
    def __init__(self, kafkahost, kafkaport, kafkatopic, groupid):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkaTopic = kafkatopic
        self.groupid = groupid
        # self.key = key
        self.consumer = KafkaConsumer(self.kafkaTopic, group_id= self.groupid,
                                      bootstrap_servers='{kafka_host}:{kafka_port}'.format(
                                          kafka_host=self.kafkaHost,
                                          kafka_port=self.kafkaPort)
                                      )

    def consumer_data(self):
        try:
            for message in self.consumer:
                yield message
        except KeyboardInterrupt as e1:
            print e1

    def main(xtype, group, key):
        if xtype == 'p':
            producer = Kafka_Producer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC, key)
            print ("===========> producer:", producer)
            for _id in range(100):
                params = '{"msg":"%s"}' % str(_id)
                params=[{"msg0":_id},{"msg1":_id}]
                producer.sendMessage(params)
                time.sleep(1)

        if xtype == 'c':
            consumer = Kafka_consumer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC, group)
            print ("===========> consumer:", consumer)
            message = consumer.consumer_data()
            for msg in message:
                print ('msg---------------->k,v', msg.key, msg.value)
                print ('offset---------------->', msg.offset)

    if __name__ == '__main__':
        xtype = sys.argv[1]
        group = sys.argv[2]
        key = sys.argv[3]
        main(xtype, group, key)