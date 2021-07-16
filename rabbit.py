import pika
import sys
import os
from dotenv import load_dotenv


def get_url():
    load_dotenv()
    username = os.environ.get('rabbitUsername')
    password = os.environ.get('rabbitPassword')
    host = os.environ.get('rabbitHost')
    virtual = os.environ.get('rabbitVirtualHost')
    return f'amqp://{username}:{password}@{host}/{virtual}'

class Consumer:
    def __init__(self):
        self._run = False
    def getConnection(self):
        self._connection = pika.BlockingConnection(pika.URLParameters(get_url()))
        print('connection open')
        if self._run:
            self.getChannel()
    def getChannel(self):
        self._channel = self._connection.channel()
        print('channel open')
        if self._run:
            self.setExchange(self._exchange, self._exchange_type)
    def setExchange(self, exchange='message', exchange_type='topic'):
        self._channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
        print('exchange declared')
        if self._run:
            self.setQueue(self._queue)
    def setQueue(self, queue='queue_name'):
        self._channel.queue_declare(queue=queue, exclusive=False)
        print('queue declared')
        if self._run:
            self.bind(self._keys, self._exchange, self._queue)
    def bind(self, keys=['routing.#'], exchange='message', queue='queue_name'):
        for k in keys:
            self._channel.queue_bind(exchange=exchange, queue=queue, routing_key=k)
        print('queue binded')
        if self._run:
            self.setConsume(self._queue, self._callback)
    def setConsume(self, queue='queue_name', callback=None):
        self._channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
        print('consume set')
        if self._run:
            self.consume()
    def consume(self):
        print('starting to consume...')
        self._channel.start_consuming()
    def run(self, exchange='message', exchange_type='topic', queue='queue_name', keys=['routing.#'], callback=None):
        self._run=True
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._queue = queue
        self._keys = keys
        self._callback= callback
        self.getConnection()
        

class Producer:
    def __init__(self):
        self._run = False
    def getConnection(self):
        self._connection = pika.BlockingConnection(pika.URLParameters(get_url()))
        print('connecion open')
        if self._run:
            self.getChannel()
    def getChannel(self):
        self._channel = self._connection.channel()
        print('channel open')
        if self._run:
            self.setExchange(self._exchange, self._exchange_type)
    def setExchange(self, exchange='message', exchange_type='topic'):
        self._channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
        print('exchange decalred')
    def publish(self, exchange='message', routing_key="uid1", msg='hello'):
        try:
            self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=msg)
        except:
            self.rerun()
            self.publish(exchange=exchange, routing_key=routing_key, msg=msg)
    def run(self, exchange='message', exchange_type='topic'):
        self._run = True
        self._exchange = exchange
        self._exchange_type = exchange_type
        self.getConnection()
    def rerun(self):
        self._run = True
        self.getConnection()