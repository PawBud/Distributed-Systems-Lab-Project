from job import Job
from compute_node import Node
from scheduler import Scheduler
from storage import StorageSystem
from cache import Cache
#All time reference are in ms
"""
def test_compute():
    job_id = "j1"
    file_id = "f1"
    file_name = "file_name_1"
    file_size = 200
    job1=job(job_id, file_id, file_name, file_size, 0)

    job2=job("j2", file_id, file_name, file_size, 5)


    nodeid = "node1"
    node1 = compute_node(nodeid)
    node1.compute(job1)
    node1.compute(job2)

    print("Jobs", job1.cumilative_time)
    print("Jobs", job2.cumilative_time)

def test_scheduler():

    job_id = "j1"
    file_id = "f1"
    file_name = "file_name_1"
    file_size = 200
    job1=job(job_id, file_id, file_name, file_size)

    job2=job("j2", file_id, file_name, file_size)

    nodeid = "n1"
    node1 = compute_node(nodeid)
    node2 = compute_node("n2")

    scheduler1 = scheduler("sch1")

    scheduler1.add_node(node1)
    scheduler1.add_node(node2)

    scheduler1.enqueue_job(1, job1)
    scheduler1.enqueue_job(1, job2)

    node_index = scheduler1.select_node(job1)
    node_index2 = scheduler1.select_node(job2)
    print("node-index", node_index, node_index2)
"""

"""
Times are in ms
Size are in bytes
"""
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


def test_scheduler_compute():
    ##Simulation parameters:
    no_of_nodes = 100
    no_of_jobs = 4000

    ##Setup simulation
    #Create storage unit
    storageObj = StorageSystem(1000) #Retrieval time(1000ms)

    #Create scheduler
    """
    Cheat sheet for hashing algorithms
    RR : Round robin
    RH : Ring hash
    MH : Mangalev
    """
    schedulerObj = Scheduler("RH")

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
        node_to_fail = random_failure_sampler()
        if node_to_fail:
            temp_job = Job("failure", "failure", 0, 0, 0, node_to_fail)
        
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
        print("[WARNING!!!] Mismatch job reported", Total_jobs_reported, no_of_jobs)

    #Cache statistics
    Cache_hit_count = 0
    Cache_miss_count = 0
    for node in schedulerObj.NodeList:
        Cache_hit_count += node.local_cache.cache_hits
        Cache_miss_count += node.local_cache.cache_miss
    
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
    print("Peak to Mean ratio of job load : ", max(Node_job_count_list)/(sum(Node_job_count_list)/len(Node_job_count_list)))
    print("Cache statistics (hits/miss) : ", Cache_hit_count, Cache_miss_count)
    print("Avg turn around time : ", Avg_TT)
    print("Time spent statistics : ", Time_spent_dict)    


"""
Changes needed:
1. Different hash algorithms [Ring hash (D), Mangelve(D), Rendzvorus]
4. Add support to get metrics [Peak to mean ratio]
5. Accomdate traces. (PDone)
6. Support for failures from traces
7. Support for restarts
8. Queue limiting scheduling policy (Opt)
9. Process with core granularity (Opt)
"""
##Main func
test_scheduler_compute()
