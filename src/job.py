#Contains charactestic of a job like
class Job:
    def __init__(self, jobId, fileId, fileSize, compute_time, Stime, node_id  = None):
        self.job_id = jobId
        self.file_id = fileId
        self.file_size = fileSize
        self.compute_time = compute_time
        self.start_time = Stime

        #In case of failure, check if needed
        self.special = node_id
        
        self.served_compute_time = 0
        self.in_storage = False
        self.allocated_storage_time = 0
        self.served_storage_time = 0
        self.end_time = -1
