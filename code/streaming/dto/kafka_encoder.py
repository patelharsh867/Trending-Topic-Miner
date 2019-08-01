import json
from streaming.dto.kafka_tweet import KafkaTweet


class KafkaEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, KafkaTweet):
            return o.__dict__
        return json.JSONEncoder.default(self, o)
