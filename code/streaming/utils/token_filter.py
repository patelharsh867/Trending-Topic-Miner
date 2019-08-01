import string

from bloom_filter import BloomFilter
from nltk import sent_tokenize
from nltk import WordNetLemmatizer
import re
import time


class TokenFilter:
    def __init__(self):
        self._bfilter = BloomFilter(max_elements=500000)
        self._populate_filter()
        self._lemmatizer = WordNetLemmatizer()

    def _populate_filter(self):
        dictionary = '/Users/jaymindesai/PycharmProjects/sketchy_data_pipelines/streaming/utils/stopwords.txt'
        start_time = time.time()
        swords = ['rt', 'co', 'http', 'ssl', 'www', 'https', 'tweet', 'retweet', 'retweets']
        with open(dictionary, 'r') as f:
            for word in f.readlines():
                self._bfilter.add(word.strip().lower())
        for word in swords:
            self._bfilter.add(word)
        print('Time taken to add words: {} seconds'.format(time.time() - start_time))

    def tokenize(self, document):
        for sent in sent_tokenize(document):
            sent = re.findall(r'[A-Za-z]+(?:[A-Za-z]+)*', sent)  # extract words
            for token in sent:
                token = token.lower()

                if token.startswith('https') or token.startswith('@'):  # remove twitter users or urls
                    continue

                if all(char in string.punctuation for char in token):  # remove trailing word punctuations
                    continue

                if token.isdigit():  # remove digits
                    continue

                if len(token) < 3:
                    continue

                # treat all the words in english dictionary as stopwords
                lemma = self._lemmatizer.lemmatize(token)
                if lemma in self._bfilter or token in self._bfilter:
                    continue

                yield token


if __name__ == '__main__':
    tf = TokenFilter()
