from elasticsearch import Elasticsearch


class ElasticService:
    def __init__(self):
        self.es_client = Elasticsearch()
        self.index = 'tweets'
        self.type = 'tweet'

    def term_stats(self, term):
        result = {}
        tweets = []
        authors = set()
        for hit in self._fetch_data(term)['hits']['hits']:
            tweets.append(hit['_source']['text'])
            authors.add(hit['_source']['author_name'])
        result['tweets'] = tweets
        result['n_tweets'] = len(tweets)
        result['n_authors'] = len(authors)
        return result

    def term_tweets(self, terms):
        result = []
        for term in terms:
            tweets = []
            response = self._fetch_data(term)
            for hit in response['hits']['hits']:
                tweets.append(hit['_source']['text'])
            result.extend(tweets)
        return result

    def delete_index(self, index):
        pass

    def _fetch_data(self, term):
        body = self._create_body(term)
        return self.es_client.search(index=self.index, doc_type=self.type, body=body)

    def _create_body(self, term):
        return {
            "query": {
                "term": {
                    "text": term
                }
            },
        }
