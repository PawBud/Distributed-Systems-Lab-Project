from cache import Cache

class Node:
    def __init__(self, nodeID, startTime, storageObj, EventHandlerObj, cacheObj = None, numCores = 1):
        #Passed as args
        self.node_id = nodeID
        self.local_time = startTime
        self.numCores = numCores

        #Init
        self.jobQ = []
        self.local_cache = cacheObj
        self.Failed = False
        self.current_ms_utlization = 0
        #For graphing
        self.current_minute_utlization = 0
        self.utl_aggr = 10**3 * 60
        self.utlization_graph = [] #Over a minute

        #Add this to config file
        self.storage = storageObj
        self.EventHandlerObj = EventHandlerObj

    def add_job(self, job):
        self.jobQ.append(job)
        
    def query_storage(self, job):
        #Cache check
        if self.local_cache is not None:
            #Check if present in cache if not add element to cache
            if self.local_cache.check(job.file_id, job.file_size):
                #Cache hit
                job.allocated_storage_time = self.local_cache.retrieval_time
            else:
                #Cache miss
                self.local_cache.add(job.file_id, job.file_size)
                job.allocated_storage_time = self.storage.get_retrieval_time(job)
        else:
            #No cache
            job.allocated_storage_time = self.storage.get_retrieval_time(job)

        job.in_storage = True
    
    def failure_flush(self):
        for job in self.jobQ:
            job.allocated_storage_time = 0
            job.served_storage_time = 0
            job.serverd_compute_time = 0
            job.in_storage = False
            self.EventHandlerObj.FailedJob(job)
            self.jobQ.remove(job)

    def Tick(self):
        #Set current utlization to zero
        self.current_ms_utlization = 0

        #If mark failed, push all jobs to schduler queue
        if self.Failed:
            self.failure_flush()
            return

        for job in self.jobQ:
            #Check job state
            #If under storage, continue
            if job.in_storage:
                job.served_storage_time += 1
                if job.served_storage_time == job.allocated_storage_time:
                    job.in_storage = False
                continue
            
            #If not, 
            #Check if node is not completely utlized
            if self.current_ms_utlization < 1 :
                #If not allocated storage
                if job.allocated_storage_time == 0:
                    self.query_storage(job)
                #increment computed time of job
                job.served_compute_time += 1
                #Increment node utlization
                self.current_ms_utlization = 1/self.numCores
            
                #Check if job is done and push to done queue
                if job.served_compute_time == job.compute_time:
                    job.compute_done = True
                    job.end_time = self.local_time + 1
                    self.EventHandlerObj.AckJob(job)
                    self.jobQ.remove(job)
                    

        #Increment local time
        self.local_time += 1
        self.current_minute_utlization += self.current_ms_utlization
        if (self.local_time+1) % self.utl_aggr == 0:
            self.utlization_graph.append(self.current_minute_utlization / self.utl_aggr)
            self.current_minute_utlization = 0