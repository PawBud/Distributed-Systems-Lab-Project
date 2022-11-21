class Cache:
    def __init__(self, node_id):
        self.cache_node_id = node_id
        self.cache_retrieval_time = 200
        self.cache_store = []
        self.cache_hits = 0
        self.cache_miss = 0
    
    def check(self, id):
        if id in self.cache_store:
            return True
        return False
    
    def add(self, id):
        pass