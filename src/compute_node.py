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
        self.utlization_ptr = [0,0] #0->Indicates ms spawned from prev minute, 1->Indicates utlization of this minute so far
        self.utlization_graph_x = [] #Have minute scale
        self.utlization_graph_y = [] #Have total utlization in that minute, i.e compute done throughout the time.

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
        #self.utlization_graph[(start_time,end_time)] = factor
        #Adding new
        #Check if start_time is in the choosen intervel
        #If not append the utl_ptr to graph; close the aggr
        #If so, 
            #Find if the given bound is with choosen minute, then change the ptr
            #If bound goes beyond, close the choosen minute by additing 
        lower_given_aggr = start_time // self.utlization_aggr
        choose_aggr = self.utlization_ptr[0] // self.utlization_aggr
        if lower_given_aggr > choose_aggr:
            self.utlization_graph_x.append(choose_aggr) #Utlization of [i,j) will be appended as ith minute
            self.utlization_graph_y.append(self.utlization_ptr[1]/self.utlization_aggr)
            if self.utlization_ptr[1] < 0:
                print("1 : ",self.utlization_ptr[1])
            choose_aggr += 1

            #Change utlization ptr
            self.utlization_ptr[0] = start_time
            self.utlization_ptr[1] = 0

        upper_given_aggr = end_time // self.utlization_aggr
        if upper_given_aggr == choose_aggr:
            #Within choosen bound
            self.utlization_ptr[1] += (end_time - start_time) 
            self.utlization_ptr[0] = end_time
        else:
            #Add choosen aggr to graph
            choosen_aggr_end_time = ((choose_aggr+1)*self.utlization_aggr) - 1
            self.utlization_ptr[1] += choosen_aggr_end_time - start_time
            self.utlization_graph_x.append(choose_aggr) #Utlization of [i,j) will be appended as ith minute
            self.utlization_graph_y.append(self.utlization_ptr[1]/self.utlization_aggr)
            if self.utlization_ptr[1] < 0:
                print("2 : ",self.utlization_ptr[1],choosen_aggr_end_time, start_time, choose_aggr, upper_given_aggr, lower_given_aggr)
            choose_aggr += 1
            """
            while choose_aggr == upper_given_aggr:
                self.utlization_graph_x.append(choose_aggr)
                self.utlization_graph_y.append(1)
            """
            choosen_aggr_end_time = (choose_aggr) * self.utlization_aggr - 1
            self.utlization_ptr[0] = end_time
            self.utlization_ptr[1] = (end_time - choosen_aggr_end_time) 
            

        
    def Run(self):
        #Do compute
        for job in self.JobQ:
            #Update local time
            timeTaken = self.compute(job)
            if job.start_time > self.local_time:
                #self.add_utlization(0, self.local_time, job.start_time) #The node was idle till job was issued
                self.local_time = job.start_time

            self.add_utlization(1/self.numCores, self.local_time, self.local_time+timeTaken) #The job was occupying one core

            self.local_time += timeTaken
            job.cumilative_time["end"] = self.local_time
            self.JobCount += 1