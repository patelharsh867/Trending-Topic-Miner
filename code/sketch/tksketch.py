from __future__ import absolute_import

import numpy as np

from sketch.pqueue import PriorityQueue


class TopKSketch(object):
    def __init__(self, k, width=200000, depth=7, dtype=np.int32, random_state=0):
        """
        Count-Min Sketch with Top K Implementation
        """
        self._k = k
        self._random_state = np.random.RandomState(random_state)
        self._items = dict()
        self._width = width
        self._depth = depth
        self._initialize_ds(dtype)
        self._size = 0
        self._pqueue = PriorityQueue()

    def __str__(self):
        return '<TopKSketch(k={})>'.format(self._k)

    def _hash(self, item, idx):
        """Hashes this item with given index"""
        info = np.iinfo(self._hash_vector.dtype)
        h = self._hash_vector[idx] * item
        h += h >> info.bits
        h &= info.max
        return h % self._width

    def _add_long(self, item, count):
        """Adds an item to the sketch and returns the minimum/maximum count"""
        h = self._hash(item, 0)
        self._sketch_table[0, h] += count
        target = self._sketch_table[0, h]
        for i in range(1, self._depth):
            h = self._hash(item, i)
            self._sketch_table[i, h] += count
            if self._sketch_table[i, h] < target:
                target = self._sketch_table[i, h]
        self._size += count
        return target

    def _hash_item(self, item):
        """Default hashing for object ids compatible with the table"""
        return hash(item) if not hasattr(item, "id") else item.id

    def _initialize_ds(self, dtype):
        self._sketch_table = np.zeros((self._depth, self._width), dtype=dtype)
        self._hash_vector = self._random_state.randint(low=0, high=np.iinfo(dtype).max, size=self._depth, dtype=dtype)

    def query(self, item):
        """Queries the sketch for frequency of this item"""
        hash_code = self._hash_item(item)
        minimum = self._sketch_table[0, self._hash(hash_code, 0)]
        for i in range(1, self._depth):
            h = self._hash(hash_code, i)
            if self._sketch_table[i, h] < minimum:
                minimum = self._sketch_table[i, h]
        return minimum

    def add(self, item):
        """Adds this item to sketch"""
        probed = None
        hash_code = self._hash_item(item)
        count = self._add_long(hash_code, 1)
        probes = [hash_code]

        for hash_code in probes:
            if hash_code in self._items:
                probed = self._items[hash_code]
                del self._items[hash_code]
            if probed is not None:
                break

        if probed is None:
            self._items[hash_code] = item
            self._pqueue.push(hash_code, count)
        else:
            pid = self._hash_item(probed)
            self._pqueue.remove(pid)
            self._items[pid] = probed
            self._pqueue.push(pid, count)

        if len(self._pqueue) > self._k:
            removed = self._pqueue.pop()
            if removed in self._items:
                del self._items[removed]

    def top_k(self, k):
        """Return top-k words from sketch"""
        return [self._items[top] for top in self._pqueue.nlargest(k)]

    def clear_sketch(self):
        self._initialize_ds(np.int32)

    def __contains__(self, query):
        return self.query(query) != 0

    def __len__(self):
        return self._size
