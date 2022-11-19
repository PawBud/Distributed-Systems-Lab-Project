import cache

class compute_node:
    def __init__(self, start_time, time, storage):
        self.JobQ = []
        self.cache = cache() 
        self.time = time #Time obj
        self.storage = storage #Storage obj

    def enqueue_job(self, job):
        self.JobQ.append(job)
    
    def dequeue_job(self):
        job = self.JobQ[0]
        del self.JobQ[0]
        job.queue_time = self.time.get_curr_ts() - job.start_time
        return job

    def compute(self, job):
        #Add compute time
        job_elapsed_time = job.compute_time
        
        #Add storage time
        storage_time = 0
        #Check cache
        if self.cache.check_cache(job.storage_id):
            storage_time += self.cache_retrivel_time
        else:
            storage_time += self.storage.request_time(job.storage_id)
       
        job_elapsed_time += storage_time
        job.compute_time = job_elapsed_time
        #Ack the job, which increments the time
    
    def start(self):
        pass
