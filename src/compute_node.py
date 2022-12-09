from cache import Cache

class Node:
    def __init__(self, nodeID, startTime, storageObj, cacheObj = None, numCores = 1):
        #Passed as args
        self.node_id = nodeID
        self.local_time = startTime
        self.numCores = numCores

        #Init
        self.JobCount = 0
        self.JobQ = []
        self.local_cache = cacheObj
        self.utl_buffer = 0
        self.utlization_graph = {}

        #Yet to use
        self.utlization_aggr = 10**3 * 60 #One minute
        self.utlization_graph_x = []
        self.utlization_graph_y = []

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
                job.add_time("cache_time", self.local_cache.retrieval_time)
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

    #Check end time bound
    def add_utlization(self, factor, start_time, end_time):
        self.utlization_graph[(start_time,end_time)] = factor

    def Run(self):
        #Do compute
        for job in self.JobQ:
            #Update local time
            timeTaken = self.compute(job)
            if job.start_time > self.local_time:
                self.add_utlization(0, self.local_time, job.start_time) #The node was idle till job was issued
                self.local_time = job.start_time

            self.add_utlization(1/self.numCores, self.local_time, self.local_time+timeTaken) #The job was occupying one core

            self.local_time += timeTaken
            job.cumilative_time["end"] = self.local_time
            self.JobCount += 1