from cache import Cache

class Node:
    def __init__(self, node_id, Stime):
        #Passed as args
        self.node_id = node_id
        self.local_time = Stime

        #Init
        self.JobQ = []
        self.local_cache = Cache(node_id)

        #Add this to config file
        self.constant_s3_storage_retrieval_time = 1000

    def add_job(self, job):
        self.JobQ.append(job)
        
    def compute(self, job):
        timeTaken = 0

        job.add_time("compute_time", job.compute_time)
        timeTaken += job.compute_time

        if self.local_cache.check(job.file_id):
            timeTaken += self.local_cache.cache_retrieval_time
            job.add_time("cpu_cache_time", self.local_cache.cache_retrieval_time)
        else:
            self.local_cache.add(job.file_id)
            timeTaken += self.constant_s3_storage_retrieval_time
            job.add_time("storage_time", self.constant_s3_storage_retrieval_time)

        return timeTaken

    def Run(self):
        for job in self.JobQ:
            timeTaken = self.compute(job)
            if job.start_time > self.local_time:
                self.local_time = job.start_time
            self.local_time += timeTaken