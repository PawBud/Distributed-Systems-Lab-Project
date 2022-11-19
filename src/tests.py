from job import job
from compute_node import compute_node
from scheduler import scheduler

def test_compute():
    job_id = "j1"
    file_id = "f1"
    file_name = "file_name_1"
    file_size = 200
    job1=job(job_id, file_id, file_name, file_size)

    job2=job("j2", file_id, file_name, file_size)


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


def test_scheduler_compute():

    file_id = "f1"
    file_name = "file_name_1"
    file_size = 200

    scheduler1 = scheduler("sch1")

    no_of_nodes = 2
    for i in range(0, no_of_nodes):
        node1 = compute_node("n"+str(i))
        scheduler1.add_node(node1)

    no_of_jobs=15
    k=0
    for i in range(0, no_of_jobs):
        if i >10:
            k=0

        temp_job=job("j"+str(i), "f"+str(k), file_name, file_size)
        scheduler1.enqueue_job(1, temp_job)

        node = scheduler1.select_node(temp_job)
        node.compute(temp_job)
        print("Jobs", temp_job.start_time, temp_job.cumilative_time)

        k+=1




test_scheduler_compute()
