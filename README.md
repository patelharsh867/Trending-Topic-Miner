### Trending Topic Miner

#### Description

Designed a scalable and fault-tolerant data pipeline using Python, Spark, Kafka and Elastic Search to get real-time top trending topics and tweets from twitter streams by implementing the count-min sketch data structure and the Heavy Hitters algorithm.

Requirements:

 - Python3.x
 - NumPy
 - Kafka 2.1.0
 - Elasticsearch 6.7.0
 - Flask
 - Client APIs and packages if not already installed:
    - pip install kafka-python
    - pip install elasticsearch
    - pip install bloom-filter
    - pip install tweepy

Setup:

 1. Start Zookeeper
        zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

 2. Start Kafka
        kafka-server-start /usr/local/etc/kafka/server.properties

 3. Create Topics:
        kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter2kafka
        kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafka2sketch

 4. Start Elasticsearch
        elasticsearch

 5. Create Elasticsearch Index
        curl -XPUT "http://localhost:9200/tweets"

 5. Navigate to the 'code' directory and start the services:
        python streaming/twitter_to_kafka.py
        python streaming/kafka_to_elastic.py
        python app/application.py

 6. Initialize the stateful count-min sketch:
        curl -v http://127.0.0.1:5000/initialize

 7. Navigate to 'web' directory:
        open topk.html

        Note: If cross-origin requests are not enabled, please first disable chrome websecurity for this session by
        running the following command (for Mac)

        open -a Google\ Chrome --args --disable-web-security --user-data-dir=""


Other details are present in the Screencast video ( [!link](https://drive.google.com/open?id=1W0o-URpjh8saD_508anorfQPIC-DV_iX) )and the presentation pdf, Thank you!


