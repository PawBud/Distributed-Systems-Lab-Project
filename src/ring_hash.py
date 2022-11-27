from ring_engine import RingEngine
from compute_node import Node
from storage import StorageSystem
from cache import Cache
class RingHash:
    def __init__(self):
        self.engine = RingEngine(10, "MURMUR3")
        self.nodeMap = {}

    def add_nodes(self, node_list):
        for node in node_list:
            pnode_id = node.node_id

            #Dont add node if already exists
            if pnode_id in self.nodeMap:
                print("Node Already exists")
                continue

            vNodes = self.engine.add_nodes(pnode_id)
            self.nodeMap[pnode_id] = vNodes

    def get_nodes(self, node_id):
        vnode_id = self.engine.get_nodes(node_id)
        print(vnode_id)


#Create nodes
no_of_nodes = 3
node_list = []
storageObj = StorageSystem(1000) #Retrieval time
for i in range(no_of_nodes):
    cacheObj = Cache("n"+str(i), 256*(1024**3), 200) #NodeId, CacheSize(256GB), RetrievalTime(200ms)
    node = Node("n"+str(i), 0, storageObj, cacheObj) #NodeId, StartTime, storageObj, cacheObj for node
    node_list.append(node)

rh = RingHash()
rh.add_nodes(node_list)
# for node, value in rh.engine.ring.items():
#     print("node", node, value.physicalNodeId)

rh.get_nodes("n2")