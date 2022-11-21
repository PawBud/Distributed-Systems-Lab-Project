#Contains charactestic of a job like
class Job:
    def __init__(self, jobId, fileId, fileSize, compute_time, Stime):
        self.job_id = jobId
        self.file_id = fileId
        self.file_size = fileSize
        self.compute_time = compute_time
        self.start_time = Stime
        
        self.special = False

        self.cumilative_time = {}
        self.end_time = -1

    def add_time(self, task_name, time):
        self.cumilative_time[task_name] = time