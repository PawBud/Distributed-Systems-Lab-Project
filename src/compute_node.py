from cache import Cache

class Node:
    def __init__(self, nodeID, startTime, storageObj, cacheObj = None):
        #Passed as args
        self.node_id = nodeID
        self.local_time = startTime

        #Init
        self.JobQ = []
        self.local_cache = cacheObj

        #Add this to config file
        self.storage = storageObj

    def add_job(self, job):
        self.JobQ.append(job)
        
    def compute(self, job):
        timeTaken = 0

        job.add_time("compute_time", job.compute_time)
        timeTaken += job.compute_time
        
        #Cache check
        if self.local_cache is not None:
            #Check if present in cache if not add element to cache
            if self.local_cache.check(job.file_id, job.file_size):
                #Cache hit
                timeTaken += self.local_cache.retrieval_time
                job.add_time("cpu_cache_time", self.local_cache.retrieval_time)
            else:
                #Cache miss
                self.local_cache.add(job.file_id, job.file_size)
                timeTaken += self.storage.get_retrieval_time(job.file_size)
                job.add_time("storage_time", self.storage.get_retrieval_time(job.file_size))
        else:
            #No cache
            timeTaken += self.storage.get_retrieval_time(job.file_size)
            job.add_time("storage_time", self.storage.get_retrieval_time(job.file_size))

        return timeTaken

    def Run(self):
        #Do compute
        for job in self.JobQ:
            #Update local time
            timeTaken = self.compute(job)
            if job.start_time > self.local_time:
                self.local_time = job.start_time
            self.local_time += timeTaken