from uhashring import HashRing
from maglev import MaglevHash
from roundRobin import RoundRobin

class HashEngine:
    def __init__(self, algo, nodes):
        self.Algo = algo
        """
        Hashing Algos
        RR -> Round robin
        RH -> Ring Hash
        MH -> Maglev      
        """
        if self.Algo == "RR":
            self.HashObj = RoundRobin()
        elif self.Algo == "RH":
            self.HashObj = HashRing()
        elif self.Algo == "MH":
            self.HashObj = MaglevHash(7)

        self.RunningNodes = []
        for node in nodes:
            self.RunningNodes.append(node.node_id)
            self.addNode(node.node_id)

    def addNode(self, nodeID):
        self.HashObj.add_node(nodeID)

    def removeNode(self, nodeID):
        self.HashObj.remove_node(nodeID)

    def lookup(self, key):
        node = self.HashObj.get_node(key)
        return node

