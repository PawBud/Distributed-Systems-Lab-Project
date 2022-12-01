import mmh3
from compute_node import Node
from virtual_node import VirtualNode

def sortRing(ring):
    sortedRing = dict(sorted(ring.items(), key=lambda item: item[0]))
    print("sortedRing",sortedRing)
    for node, value in sortedRing.items():
        print("node", node, value.physicalNodeId)

    return sortedRing


class RingEngine:
    def __init__(self, noNodes):
        self.vnodes = noNodes
        self.ring = {} #Int:VirtualNode -> key:value

    def add_nodes(self, pnode_id):
        vNodes = []
        for i in range(0, self.vnodes):
            hashF = mmh3.hash(str(i)+" "+pnode_id)
            while hashF in self.ring:
                hashF = mmh3.hash(str(i)+" "+pnode_id)

            vcomputeNode = VirtualNode(pnode_id, hashF)
            self.ring[hashF] = vcomputeNode
            vNodes.append(vcomputeNode)

        return vNodes

    def get_nodes(self, pnode_id):
        hashP = mmh3.hash(pnode_id)
        sortedRing = sortRing(self.ring)
        for hashKey, VNode in sortedRing.items():
            if hashP >= hashKey:
                print("hashP", hashP, hashKey)
                return VNode.physicalNodeId


    def remove_nodes(self, hash_node_id):
        for nodeKey in self.ring:
            if nodeKey == hash_node_id:
                self.pop(nodeKey)
