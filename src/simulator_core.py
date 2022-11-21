class pseudo_time:
    def __init__(self):
        self.current_time = 0

    def tick(self, ts, qntms):
        #Increment current timestamp
        finish_time = ts + qntms
        if finish_time > self.current_time:
            self.current_time = finish_time
        
    def get_current_time(self):
        return self.current_time