from cache import cache
from job import job
import asyncio

class compute_node():
    def __init__(self, node_id):
        self.node_id = node_id
        self.JobQ = []
        self.cache = cache(node_id)
        self.local_cache = self.cache.cache_store
        self.node_busy = False
        self.queue_busy = False

        #Add this to config file
        self.constant_compute_time = 500
        self.constant_cache_retrieval_time = 200
        self.constant_s3_storage_retrieval_time = 1000


    def enqueue_job(self, job, scheduler1):
        self.JobQ.append(job)
        if not self.queue_busy:
            self.compute_from_queue(scheduler1)
    
    def dequeue_job(self):
        job = self.JobQ[0]
        del self.JobQ[0]
        job.queue_time = self.time.get_curr_ts() - job.start_time
        return job
        
    def compute_from_queue(self, scheduler):
        if len(self.JobQ) > 0:
            self.queue_busy = True
            job = self.dequeue_job()
            asyncio.run(self.compute(job, scheduler))

        if len(self.JobQ) > 0:
            self.compute_from_queue()
        else:
            self.queue_busy = False
    
    async def compute(self, job, scheduler1):
        self.node_busy = True
        job.add_time("compute_time", self.constant_compute_time)
        await asyncio.sleep(0.5) #Test line
        
        #Check cache
        if job.file_id in self.local_cache:
            job.add_time("cpu_cache_time",self.constant_cache_retrieval_time)
        else:
            self.local_cache.append(job.file_id)
            job.add_time("storage_time",self.constant_s3_storage_retrieval_time) 
        
        self.node_busy = False
        scheduler1.enqueue_job(0, job) #Add to ack jobs
        
        return job
