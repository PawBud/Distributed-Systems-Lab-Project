from cache import cache
from job import job

class compute_node():
    def __init__(self, node_id):
        self.node_id = node_id
        self.JobQ = []
        self.cache = cache(node_id)
        self.local_cache = self.cache.cache_store
        self.node_busy = False

        #Add this to config file
        self.constant_compute_time = 500
        self.constant_cache_retrieval_time = 200
        self.constant_s3_storage_retrieval_time = 1000


    def enqueue_job(self, job):
        self.JobQ.append(job)
    
    def dequeue_job(self):
        #FIXME: Should add queue time!
        job = self.JobQ[0]
        del self.JobQ[0]
        return job
    
    def compute(self, job):
        
        self.node_busy = True
        job.add_time("compute_time", self.constant_compute_time)

        #Check cache
        if job.file_id in self.local_cache:
            job.add_time("cpu_cache_time",self.constant_cache_retrieval_time)
        else:
            self.local_cache.append(job.file_id)
            job.add_time("storage_time",self.constant_compute_time)
        
        self.node_busy = False


        return True


