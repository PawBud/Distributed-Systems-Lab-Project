class RoundRobin:
    def __init__(self):
        self.nodes = []
        self.curr_ptr = 0
    def add_node(self, nodeId):
        self.nodes.append(nodeId)
    def remove_node(self, nodeId):
        self.nodes.remove(nodeId)
    def get_node(self, key):
        node = self.nodes[self.curr_ptr%len(self.nodes)]
        self.curr_ptr += 1
        return node
