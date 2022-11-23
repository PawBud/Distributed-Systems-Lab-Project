"""
hash_policy_active = False
def simple_file_hash_policy(fileHashes, job):
        for file_id in fileHashes:
            if file_id == job.file_id:
                currentNodeIndex = fileHashes[file_id]
                return currentNodeIndex
                
        return -1

def round_robin_policy(last_node_index, node_list):
    if last_node_index >= len(node_list):
        last_node_index = 0

    return last_node_index
"""

class Scheduler:
    def __init__(self):
        self.JobQ = []
        #self.fileHashes = {}
        self.NodeList = []
        self.RunningNodes = []
        self.curr_node = 0
        #self.last_node_index = 0
        #self.max_hash_entries = 10

    def add_node(self, node):
        self.NodeList.append(node)
    
    def add_job(self, job):
        self.JobQ.append(job)

    def select_node(self, job):
        #Simple File hash Policy that checks if a file_id has an entry
        """
        node_index = simple_file_hash_policy(self.fileHashes, job)
        if node_index != -1:
            print("Simple Hash - index:", node_index)
            return self.NodeList[node_index]

        print("last_node_index", self.last_node_index)
        node_index = round_robin_policy(self.last_node_index, self.node_list)
        if node_index != -2:
            print("round robin - index:", node_index)
            if hash_policy_active:
                if len(self.fileHashes) >= self.max_hash_entries:
                    #This will delete the first entry in the hash dictionary
                    (k := next(iter(self.fileHashes)), self.fileHashes.pop(k))

                self.fileHashes[job.file_id] = node_index
            self.last_node_index=node_index+1
            return self.NodeList[node_index]
        """
        #Round robin
        node = self.RunningNodes[self.curr_node]
        self.curr_node = (self.curr_node + 1)%len(self.RunningNodes)
        return node

    def Run(self):
        for job in self.JobQ:
            
            #Get node for the job
            node = self.select_node(job)

            #check if special job(Failure)
            if job.special != None:
                for node in self.RunningNodes:
                    if node.node_id == job.special:
                        print("removed node id", node.node_id)
                        self.RunningNodes.remove(node)
                        continue

            #Append to jobQ of the node
            node.add_job(job)



    


      
        
       



