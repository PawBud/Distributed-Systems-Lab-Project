import time

#Contains charactestic of a job like
class job:
    def __init__(self, jobId, fileId, fileName, fileSize):
        self.job_id = jobId
        self.file_id = fileId
        self.file_name = fileName
        self.file_size = fileSize
        self.start_time = time.time()
        self.end_time = time.time()
        self.cumilative_time = {}


    def add_time(self, task_name, time):
        self.cumilative_time[task_name] = time