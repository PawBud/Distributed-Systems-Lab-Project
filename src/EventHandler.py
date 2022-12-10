
from job import Job
class EventHandler:
    def __init__(self):
        #Provided directly
        self.Schduler = None
        self.failures = None

        self.JobQ=[]
        self.JobCount = 0
        self.FailedJobs=[]
        self.AckedJobs = 0

        self.local_time = 0
        self.SimulationDone = False


    def addJobs(self, jobs):
        self.JobQ = jobs
        self.JobCount = len(jobs)

    def AckJob(self, job):
        self.AckedJobs += 1

    def FailedJob(self, job):
        self.FailedJobs.append(job)

    def Tick(self):
        pass

        if self.AckedJobs == self.JobCount:
            self.SimulationDone = True
            return
        
        #Scdule failures
        if self.failures != None:
            if self.local_time == self.failures[0]:
                #Fail self.failures[1] node
                #TODO: Failure job
                failure_job = Job()
                self.Schduler.add_job(failure_job)

        #Schdule failed jobs first
        for job in self.FailedJobs:
            self.Schduler.add_job(job)
            self.FailedJob.remove(job)

        #Schdule remining jobs
        for job in self.JobQ:
            if job.start_time <= self.local_time:
                print("Job added : ",job.job_id)
                self.Schduler.add_job(job)
                self.JobQ.remove(job)

        #Increment time
        self.local_time += 1
