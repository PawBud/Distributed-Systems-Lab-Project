from hash_engine import HashEngine

class Scheduler:
    def __init__(self, algo):
        self.JobQ = []
        self.NodeList = []
        self.Algo = algo

    def add_node(self, node):
        self.NodeList.append(node)
    
    def add_job(self, job):
        self.JobQ.append(job)

    def select_node(self, job):
        #Get nodeID from hashObj
        nodeId = self.HashObj.lookup(job.file_id)
        #Get node from list
        for node in self.NodeList:
            if node.node_id == nodeId:
                return node


    def Run(self):
        #Init hash obj
        self.HashObj = HashEngine(self.Algo, self.NodeList)

        #Assign jobs
        for job in self.JobQ:
            #check if special job(Failure)
            if job.special != None:
                for node in self.NodeList:
                    if node.node_id == job.special:
                        print("removed node id", node.node_id)
                        self.HashObj.removeNode(node.node_id)
                        continue


            #Get node for the job
            node = self.select_node(job)
            #Append to jobQ of the node
            node.add_job(job)



    


      
        
       



