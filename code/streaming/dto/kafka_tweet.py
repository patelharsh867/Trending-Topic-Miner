class KafkaTweet(object):
    def __init__(self, text, author_name, author_handle, date_created, location):
        self.text = text
        self.author_name = author_name
        self.author_handle = author_handle
        self.date_created = date_created
        self.location = location
