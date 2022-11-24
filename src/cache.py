#Changes for multiple and size on cache check
class Cache:
    def __init__(self, node_id, size, retrieval_time):
        self.cache_node_id = node_id
        self.retrieval_time = retrieval_time
        self.cache_size = size
        self.cache_curr_size = 0
        self.cache_store = {} #Dict of id as key, size as value
        self.cache_order = []

        #Metric of cache
        self.cache_hits = 0
        self.cache_miss = 0
    
    def check(self, id, size):
        if id in self.cache_store.keys():
            self.cache_hits += 1
            return True
        self.cache_miss += 1
        return False
    
    def add(self, id, size):
        #Check size if needed eviction
        #FIFO replacement algorithm
        while self.cache_curr_size + size > self.cache_size:
            evict_id = self.cache_order[0]
            del self.cache_order[0]
            evict_size = self.cache_store[evict_id]
            del self.cache_store[evict_id]
            self.cache_curr_size -= evict_size

        #Add id to cache
        self.cache_order.append(id)
        self.cache_store[id] = size
        self.cache_curr_size += size

    def reset(self):
        self.cache_order = []
        self.cache_store = {}
        self.cache_curr_size = 0