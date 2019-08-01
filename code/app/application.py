from kafka import KafkaConsumer
from sketch.tksketch import TopKSketch
from app.elastic_service import ElasticService
from flask import Flask
from flask.json import jsonify

app = Flask(__name__)
elastic_service = ElasticService()


@app.route("/initialize")
def initialize_sketch():
    topic = 'kafka2sketch'

    consumer = KafkaConsumer(topic,
                             auto_offset_reset='latest',
                             bootstrap_servers=['localhost:9092'],
                             consumer_timeout_ms=10000)

    for msg in consumer:
        tks.add(str(msg.value.decode('utf-8')))

    if consumer is not None:
        consumer.close()

    print('KHH Service Started')
    return '[HTTP Status: 200] KHH Service Started'


@app.route("/topk/<int:k>")
def get_topk(k):
    return jsonify(tks.top_k(k))


@app.route("/toptweets")
def get_top_tweets():
    top_terms = tks.top_k(5)
    result = elastic_service.term_tweets(top_terms)
    return jsonify(result)


@app.route("/termstats/<string:term>")
def get_term_stats(term):
    result = elastic_service.term_stats(term)
    return jsonify(result)


@app.route("/clear")
def get_clear_sketch():
    tks.clear_sketch()
    return '[HTTP Status: 200] Sketch Cleared'


@app.route("/deleteindex/<string:index>")
def get_delete_index(index):
    elastic_service.delete_index(index)
    return '[HTTP Status: 200] Index Cleared'


if __name__ == '__main__':
    tks = TopKSketch(50)
    app.run(host='0.0.0.0')
