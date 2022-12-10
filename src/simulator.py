class Simulator:
    def __init__(self, components):
        self.components = components #Passed in order of excution [Event Inducer, Schduler, Nodes in any order, Storage and Failure Handler Component]
        self.Total_Runtime = 0
        
    def Run(self):
        for component in self.components:
            component.Tick()
        self.Total_Runtime += 1