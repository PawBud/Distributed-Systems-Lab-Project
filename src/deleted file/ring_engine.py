import mmh3

class RingEngine:
    def __init__(self, noNodes):
        self.vnodes = noNodes
        self.ring = {} #Int:VirtualNode -> key:value

    def add_nodes(self, pnode_id):
        vNodes = []
        for i in range(0, self.vnodes):
            hashF = mmh3.hash(pnode_id+str(i))
            while hashF in self.ring:
                hashF = mmh3.hash(pnode_id+str(i))

            self.ring[hashF] = computeNode
            vNodes.append(computeNode)

        return vNodes

    def remove_nodes(self, hash_node_id):
        for nodeKey in self.ring:
            if nodeKey == hash_node_id:
                self.pop(nodeKey)
