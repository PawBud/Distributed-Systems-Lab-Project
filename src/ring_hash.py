from ring_engine import RingEngine
from compute_node import Node

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

#Create nodes
no_of_nodes = 3
node_list = []
for i in range(no_of_nodes):
    node = Node("n"+str(i), 0)
    node_list.append(node)

rh = RingHash()
rh.add_nodes(node_list)
for node, value in rh.engine.ring.items():
    print("node", node, value.node_id)