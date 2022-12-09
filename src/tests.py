from job import Job
from compute_node import Node
from scheduler import Scheduler
from storage import StorageSystem
from cache import Cache
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
"""
Times are in millisec
Size are in bytes
"""


"""
Accomdating trace
Traces has following fields:
<time stamp of request>, <request type>, <object ID>, <size of object>
We can omit request type
Time stamp should start from 0
Sample job run time from distribition
"""
def get_jobs_from_trace(path):
    f = open(path, "r")
    trace_list = f.readlines()
    JobList = []
    initTime = int(trace_list[0].split(" ")[0]) #Get start time of first and subtract it
    n = len(trace_list)  # sample size
    mu = 500  # mean
    sigma = 10  # sd
    a = np.random.normal(mu,sigma,n)
    sns.displot(a, kind="ecdf") #Distribution over
    plt.show()
    i = 0
    for trace in trace_list:
        trace_info = trace.split(" ")
        if len(trace_info) >= 4:
            startTime = (int(trace_info[0]) - initTime)#Init trace as 0
            fileId = trace_info[2]
            fileSize = int(trace_info[3]) 
            #TODO:Sample computation_time from distribution
            compute_time = int(a[i])
            tempObj = Job("j"+str(len(JobList)), fileId, fileSize, compute_time, startTime)
            JobList.append(tempObj)
            if len(JobList)==-1:
                temp_job = Job("failure", "failure", 0, 0, 0, "n0")
                JobList.append(temp_job)
            i += 1
    return JobList


NodeList = []
Failure_probability = 0
switch = False
def random_failure_sampler():
    #When called return node to fail probabilstically
    global switch
    if not switch:
        return None
    switch = False
    return "n2"


def print_results(schedulerObj, no_of_jobs):
    ##Collect results
    #Run time
    Total_time = 0
    for node in schedulerObj.NodeList:
        if Total_time < node.local_time:
            Total_time = node.local_time
    
    #Load of nodes (Peak to Mean)
    Total_jobs_reported = 0
    Node_job_count_list = []
    for node in schedulerObj.NodeList:
        Total_jobs_reported += node.JobCount
        Node_job_count_list.append(node.JobCount)
    if Total_jobs_reported != no_of_jobs:
        print("[WARNING] Mismatch job reported!!", Total_jobs_reported, no_of_jobs)

    #Cache statistics
    Cache_hit_count = 0
    Cache_miss_count = 0
    node_list = []
    cache_hit_list = []
    cache_miss_list = []
    for node in schedulerObj.NodeList:
        Cache_hit_count += node.local_cache.cache_hits
        Cache_miss_count += node.local_cache.cache_miss
        node_list.append(node.node_id)
        cache_hit_list.append(node.local_cache.cache_hits)
        cache_miss_list.append(node.local_cache.cache_miss)
    #Time spent
    Time_spent_dict = {}
    Total_TT = 0
    for job in schedulerObj.JobQ:
        tt = job.cumilative_time["end"] - job.cumilative_time["start"]
        for key in job.cumilative_time.keys():
            if key in ["start", "end"]:
                continue
            try:
                Time_spent_dict[key] += job.cumilative_time[key]
            except:
                Time_spent_dict[key] = job.cumilative_time[key]
        Total_TT += tt
    Avg_TT = Total_TT / no_of_jobs
        

    ##Show stats
    print("Time taken to complete (ms) : ", Total_time)
    print("Peak to Mean ratio of job load : ", Node_job_count_list ,max(Node_job_count_list)/(sum(Node_job_count_list)/len(Node_job_count_list)))
    print("Cache statistics (hits/miss) : ", Cache_hit_count, Cache_miss_count)
    print("Avg turn around time : ", Avg_TT)
    print("Time spent statistics : ", Time_spent_dict)

    #Utlization graph
    figure, axis = plt.subplots(len(schedulerObj.NodeList), 1)
    c = 0
    for i in schedulerObj.NodeList:
        graph = i.utlization_graph
        x=[]
        y=[]
        for i in graph.keys():
            x.extend([j for j in range(i[0],i[1])])
            y.extend([graph[i] for j in range(i[0],i[1])])
        axis[c].plot(x,y)
        axis[c].set_xlim(0,Total_time)
        c +=  1
    plt.show()

    #Cache statistics graph
    plt.bar(node_list, cache_hit_list, color='b')
    plt.bar(node_list, cache_miss_list, color='orange')
    plt.show()

def test_case_1(no_of_nodes, no_of_jobs, algo):
    ##Setup simulation
    #Create storage unit
    storageObj = StorageSystem(1000) #Retrieval time(1000ms)

    #Create scheduler
    schedulerObj = Scheduler(algo)
    #Create nodes
    for i in range(no_of_nodes):
        cacheObj = Cache("n"+str(i), 256*(1024**3), 200) #NodeId, CacheSize(256GB), RetrievalTime(200ms)
        node = Node("n"+str(i), 0, storageObj, cacheObj) #NodeId, StartTime, storageObj, cacheObj for node
        schedulerObj.add_node(node)

    #creating a copy of the node list
    schedulerObj.RunningNodes = schedulerObj.NodeList[:]

    #Create jobs
    for i in range(no_of_jobs):
        temp_job = Job("j"+str(i), "f"+str(1), 10*(1024**3), 60*(10**3), 0) #Job id, file_id, file_size, job_compute_time, job_start_time
       
        #Inclusion of failure job, set check to -1 if dont want failures
        """
        Failing is done on job granularity, For now, we cant fail a node inbetween job execution
        """
        #TODO: Random failure of nodes
        #node_to_fail = random_failure_sampler()
        if i==-1:
            temp_job = Job("failure", "failure", 0, 0, 0, "n0")
        
        #Add job to schedulerQueue
        schedulerObj.add_job(temp_job)


    ##Run the simulation
    """
    First scheduler assigns jobs to all the nodes.
    A special case on node failure, Failure is a special job that stops a node. Failures in granularity of jobs 
    """
    #Scheduler
    schedulerObj.Run()
    #Run node(s)
    for node in schedulerObj.NodeList:
        node.Run()

    ##Print results
    print_results(schedulerObj, no_of_jobs)
      

def test_case_2(no_of_nodes, path, algo):
    ##Setup simulation
    #Create storage unit
    storageObj = StorageSystem(1000) #Retrieval time(1000ms)

    #Create scheduler
    schedulerObj = Scheduler(algo)
    #Create nodes
    for i in range(no_of_nodes):
        cacheObj = Cache("n"+str(i), 256*(1024**3), 200) #NodeId, CacheSize(256GB), RetrievalTime(200ms)
        node = Node("n"+str(i), 0, storageObj, cacheObj) #NodeId, StartTime, storageObj, cacheObj for node
        schedulerObj.add_node(node)

    #creating a copy of the node list
    schedulerObj.RunningNodes = schedulerObj.NodeList[:]

    #Create jobs
    jobs = get_jobs_from_trace(path)
    for job in jobs:      
        #Add job to schedulerQueue
        schedulerObj.add_job(job)


    ##Run the simulation
    """
    First scheduler assigns jobs to all the nodes.
    A special case on node failure, Failure is a special job that stops a node. Failures in granularity of jobs 
    """
    #Scheduler
    schedulerObj.Run()
    #Run node(s)
    for node in schedulerObj.NodeList:
        node.Run()

    ##Print results
    print_results(schedulerObj, len(jobs))

"""
Changes needed:
1. Different hash algorithms [Ring hash (D), Maglev(D)]
2. Add support to get metrics [Peak to mean ratio, Cache hit ratio] (D)
3. Accomdate traces (D)
4. Support for failures from traces
"""
##Main func
"""
Cheat sheet for hashing algorithms
RR : Round robin
RH : Ring hash
MH : Maglev
AH : Anchor hash
RZ : redz 
BS : Bloom filter based (Addition to other class)
Test cases:
test_case_1(no_of_nodes, no_of_jobs, algo) #Random job creation
"""
"""
List of experiments:
1. Peak to mean ratio (Overprovisioning)
2. Rebalancing [No of objects moved, increase in job response time, Time to steady state]
3. Same experiment failures
4. Simulated jobs with same file as input
"""
test_case_2(2, "Traces/Trace1", "RH")
