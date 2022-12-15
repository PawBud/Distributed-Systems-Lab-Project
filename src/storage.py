# import matplotlib.pyplot as plt
# import numpy as np

class StorageSystem:
    def __init__(self, retrieval_time):
        self.retrieval_time = retrieval_time
    def get_retrieval_time(self, size):
        # Incoming Size in bytes
        # We take our figures from http://www.paralleldatageneration.org/download/wbdb/WBDB2012_IN_15_Panda_HDFSBenchmark.pdf
        # We generalize our throughput as 1400 MBps
        size_in_bytes = 1400*1024*1024
        # time in ms !!
        tot_time = 0
        tot_time += (size/size_in_bytes)*1400
        # Modelling the Latency
        # Max File Size = 10 GB
        # x = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        # y = [2, 5, 8, 10, 15, 18, 26, 30, 33, 39, 44]
        # plt.plot(x, y)
        # tot_time += np.interp(size/(1024*1024*1024), x, y) * 1000
        # We get the equation as y = 4.34x - 0.86
        latency = 4.34*(size/size_in_bytes) - 0.86
        tot_latency = 0
        if(latency < 0):
            tot_latency = 4.34*(size/size_in_bytes)
        else:
            tot_latency = latency*1000
        tot_time += tot_latency*1000
        print("tot_time: ", tot_time)
        # Counting time which is less than 1 ms as 0
        return int(tot_time)