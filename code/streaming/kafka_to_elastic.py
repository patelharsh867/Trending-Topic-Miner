from kafka import KafkaConsumer, KafkaProducer
import json
from time import sleep
from elasticsearch import Elasticsearch
from streaming.utils.token_filter import TokenFilter


class ElasticRepository:

    def __init__(self, from_topic, to_topic, index, type):
        self.from_topic = from_topic
        self.to_topic = to_topic
        self.index = index
        self.type = type
        self.consumer = self._init_consumer()
        self.producer = self._init_producer()
        self.es = Elasticsearch()
        self.token_filter = TokenFilter()

    def _init_consumer(self):
        return KafkaConsumer(self.from_topic,
                             auto_offset_reset='latest',
                             bootstrap_servers=['localhost:9092'],
                             consumer_timeout_ms=100000)

    def _init_producer(self):
        return KafkaProducer(bootstrap_servers=['localhost:9092'], batch_size=1000, linger_ms=10)

    def start_service(self):
        es = Elasticsearch()
        for msg in self.consumer:
            record = json.loads(msg.value)
            es.index(index=self.index, doc_type=self.type, body=record)
            for token in self.token_filter.tokenize(record['text']):
                self.producer.send(self.to_topic, bytes(token, 'utf-8'))
            sleep(1)

        if self.consumer is not None:
            self.consumer.close()


if __name__ == '__main__':
    elastic_repo = ElasticRepository('twitter2kafka', 'kafka2sketch', 'tweets', 'tweet')
    elastic_repo.start_service()
