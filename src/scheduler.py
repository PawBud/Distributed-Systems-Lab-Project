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

class scheduler:
    def __init__(self, schedulerId):
        self.scheduler_id = schedulerId
        self.EntryQueue = []
        self.ExitQueue = []
        self.fileHashes = {}
        self.node_list = []
        self.last_node_index = 0

        self.max_hash_entries = 10

    def add_node(self, node):
        self.node_list.append(node)

    def enqueue_job(self, queue_type, job):

        if queue_type == 1:
            self.EntryQueue.append(job)
        else:
            self.ExitQueue.append(job)

    
    def dequeue_job(self, queue_type):
        #FIXME: Should add queue time!
        job
        if queue_type == 1:
            job = self.EntryQueue[0]
            del self.EntryQueue[0]
        else:
            job = self.ExitQueue[0]
            del self.ExitQueue[0]
        return job

    
    
    def select_node(self, job):

        #Simple File hash Policy that checks if a file_id has an entry
        node_index = simple_file_hash_policy(self.fileHashes, job)
        if node_index != -1:
            print("Simple Hash - index:", node_index)
            return self.node_list[node_index]

        node_index = round_robin_policy(self.last_node_index, self.node_list)
        if node_index != -1:
            print("round robin - index:", node_index)
            if hash_policy_active:
                if len(self.fileHashes) >= self.max_hash_entries:
                    #This will delete the first entry in the hash dictionary
                    (k := next(iter(self.fileHashes)), self.fileHashes.pop(k))

                self.fileHashes[job.file_id] = node_index
            self.last_node_index+=1
            return self.node_list[node_index]

        return -1

        
       



