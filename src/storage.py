class StorageSystem:
    def __init__(self, retrieval_time):
        self.retrieval_time = retrieval_time
    def get_retrieval_time(self, size):
        #Constant retrival time for now!
        return self.retrieval_time