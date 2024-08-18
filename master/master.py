import grpc
import os
import sys


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import task_pb2
import task_pb2_grpc

class Master:
    def __init__(self):

        self.channel = grpc.insecure_channel('localhost:50051')
        self.stub = task_pb2_grpc.WorkerStub(self.channel)

    def distribute_task(self, csv_file):

        with open(csv_file, 'rb') as f:
            data = f.read()
        
        request = task_pb2.AnalysisRequest(data=data)
        
        try:
            response = self.stub.AnalyzeData(request)
            print(f"Response from worker: {response.result}")
        except grpc.RpcError as e:
            print(f"gRPC error occurred: {e}")

if __name__ == '__main__':

    master = Master()
    
    csv_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'COVID_Death_USA.csv')
    
    master.distribute_task(csv_file)
