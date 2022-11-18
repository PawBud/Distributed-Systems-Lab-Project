class compute_node:
    def __init__(self):
        pass
        self.JobQ = []
        self.cache = cache
    def enqueue_job(self, job):
        self.JobQ.append(job)
    
    def dequeue_job(self):
        #FIXME: Should add queue time!
        job = self.JobQ[0]
        del self.JobQ[0]
        return job
    
    def compute(self, job):
        job.add_elapsed_time(job.compute_time)
        storage_time = 0
        #Check cache
        if self.cache.check_cache(job.storage_id):
            storage_time += self.cache_retrivel_time
        else:
            storage_time += self.storage.request_time(job.storage_id)
        job.add_elapsed_time(storage_time)
