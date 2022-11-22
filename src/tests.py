from job import Job
from compute_node import Node
from scheduler import Scheduler


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

def test_scheduler_compute():

    ##Setup simulation
    #Create scheduler
    schedulerObj = Scheduler()

    #Create nodes
    no_of_nodes = 3
    for i in range(no_of_nodes):
        node = Node("n"+str(i), 0)
        schedulerObj.add_node(node)

    #Create jobs
    no_of_jobs=3
    for i in range(no_of_jobs):
        temp_job = Job("j"+str(i), "f"+str(i), 200, 1000, 1000*i) #Job id, file id, file_size, job_compute_time, job_start_time
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
2. Cache implmentation
3. Storage implmentaion to accomdate size based and dynamic latency
4. Add support to get metrics 
5. Accomdate traces.
6. Support for failures and restart
7. Queue limiting scheduling policy (Opt)
8. Process with core granularity (Opt)
"""
##Main func
test_scheduler_compute()
