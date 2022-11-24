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
def test_scheduler_compute():
    ##Simulation parameters:
    no_of_nodes = 5
    no_of_jobs = 10

    ##Setup simulation
    #Create storage unit
    storageObj = StorageSystem(1000) #Retrieval time

    #Create scheduler
    schedulerObj = Scheduler()

    #Create nodes
    for i in range(no_of_nodes):
        cacheObj = Cache("n"+str(i), 256*(1024**3), 200) #NodeId, CacheSize(256GB), RetrievalTime(200ms)
        node = Node("n"+str(i), 0, storageObj, cacheObj) #NodeId, StartTime, storageObj, cacheObj for node
        schedulerObj.add_node(node)

    #creating a copy of the node list
    schedulerObj.RunningNodes = schedulerObj.NodeList[:]

    #Create jobs
    for i in range(no_of_jobs):
        temp_job = Job("j"+str(i), "f"+str(0), 10*(1024**3), 60*(10**3), 0) #Job id, file id, file_size, job_compute_time, job_start_time
       
        #Inclusion of failure job, set check to -1 if dont want failures
        """
        Failing is done on job granularity, For now, we cant fail a node inbetween job execution
        """
        if i == -1:
            temp_job = Job("failure", "failure", 0, 0, 0, "n1")
        
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
    time = -1
    for node in schedulerObj.NodeList:
        if time < node.local_time:
            time = node.local_time
            

    print("Time taken to complete (ms) : ", time)    


"""
Changes needed:
1. Different hash algorithms 
2. Cache implmentation (Done)
3. Storage implmentaion to accomdate size based and dynamic latency
4. Add support to get metrics 
5. Accomdate traces. (PDone)
6. Support for failures (Done)
7. Support for restarts
8. Queue limiting scheduling policy (Opt)
9. Process with core granularity (Opt)
"""
##Main func
test_scheduler_compute()
