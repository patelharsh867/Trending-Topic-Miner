from kafka import KafkaProducer
import tweepy
from streaming.dto.kafka_tweet import KafkaTweet
from streaming.dto.kafka_encoder import KafkaEncoder
from streaming.utils.config_parser import read_config


class TweeterStreamListener(tweepy.StreamListener):
    """Consume Twitter APIs to fetch real time tweets and dump them to Kafka"""

    def __init__(self, api, topic):
        super(tweepy.StreamListener, self).__init__()
        self.api = api
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], batch_size=1000, linger_ms=10)
        self.encoder = KafkaEncoder()
        self.topic = topic

    def on_status(self, tweet):
        """This method is called whenever new data arrives from live stream. This data is
        asynchronously pushed to a kafka topic."""

        text = tweet.text  # tweet
        name = tweet.author.name
        handle = tweet.author.screen_name
        location = tweet.author.location
        date = tweet.timestamp_ms

        kf_tweet = self._encode(KafkaTweet(text, name, handle, date, location))  # encode to json format

        try:
            self.producer.send(self.topic, kf_tweet)
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True  # Keep the stream alive

    def on_timeout(self):
        return True  # Keep the stream alive

    def _encode(self, data):
        return self.encoder.encode(data).encode('utf-8')


if __name__ == '__main__':
    config = read_config('../streaming/properties/config.properties')

    access_key = config['access_key']
    access_secret = config['access_secret']
    consumer_key = config['consumer_key']
    consumer_secret = config['consumer_secret']

    # Create Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # Create stream and bind the listener to it
    topic = 'twitter2kafka'
    stream = tweepy.Stream(auth, listener=TweeterStreamListener(api, topic=topic))

    # Custom Filter rules pull all traffic for those filters in real time.
    INDIA = [68.11137867, 6.55460784, 97.39556094, 35.67454562]
    # NC = [-84.32186902, 33.75287798, -75.40011906, 36.58803627]
    # stream.filter(track=['tweet'], languages=['en'])
    stream.filter(locations=INDIA)
