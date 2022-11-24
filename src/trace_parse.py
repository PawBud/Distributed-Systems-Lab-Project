class Trace:
    def __init__(self, traceId, startTime, reqType, fileId, fileSize, lookStart, lookEnd):
        self.trace_id = traceId
        self.start_time = startTime
        self.req_type = reqType
        self.file_id = fileId
        self.file_size = fileSize
        self.lookup_start = lookStart
        self.lookup_end = lookEnd


def read_trace_file(path):
    f = open(path, "r")
    file_content = f.read()

    trace_list = file_content.split("\n")
    return trace_list
    
def create_trace_objects(path):
    trace_list = read_trace_file(path)
    traceObjs = []
    for trace in trace_list:
        trace_info = trace.split(" ")
        if len(trace_info) > 1:
            startTime = trace_info[0]
            reqType = trace_info[1]
            fileId = trace_info[2]
            fileSize = trace_info[3]
            lookStart = None
            lookEnd = None
            if len(trace_info) > 4:
                lookStart = trace_info[4]
                lookEnd = trace_info[5]

            temptraceObj = Trace("t"+str(len(traceObjs)), startTime, reqType, fileId, fileSize, lookStart, lookEnd)
            traceObjs.append(temptraceObj)

    return traceObjs
