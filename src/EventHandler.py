
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

        self.job_frequency_graph = []
        self.job_count_in_current_aggr = 0
        self.job_freq_aggr = (10**3)*60


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
                print("Failed job")
                failure_job = Job("failure", "failure", 0, 0, 0, self.failures[1])
                self.Schduler.add_job(failure_job)

        #Schdule failed jobs first  
        for job in self.FailedJobs:
            self.Schduler.add_job(job)
            self.FailedJobs.remove(job)

        #Schdule remining jobs
        job_added = 0
        for job in self.JobQ:
            if job.start_time <= self.local_time:
                #print("Job added : ",job.job_id)
                self.Schduler.add_job(job)
                self.JobQ.remove(job)
                job_added += 1
            else:
                break
                

        #Increment time
        self.local_time += 1

        #Note job frequecy
        self.job_count_in_current_aggr += job_added
        if (self.local_time+1) % self.job_freq_aggr == 0:
            self.job_frequency_graph.append(self.job_count_in_current_aggr)
            self.job_count_in_current_aggr = 0
