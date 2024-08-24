import sys
import os

worker_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../worker'))
sys.path.append(worker_path)

print(f"Current sys.path: {sys.path}")

import grpc
import worker.task_pb2 as task_pb2
import worker.task_pb2_grpc as task_pb2_grpc

def run_worker_analysis(file_path):
    with grpc.insecure_channel('localhost:50052') as channel:
        stub = task_pb2_grpc.WorkerStub(channel)
        
        try:
            with open(file_path, 'rb') as f:
                data = f.read()
            response = stub.AnalyzeData(task_pb2.AnalysisRequest(data=data))
            print("Server Response:")
            print(response.result)

        except FileNotFoundError:
            print("CSV file not found. Check the path.")
        except grpc.RpcError as e:
            print(f"gRPC error: {e.code()} - {e.details()}")

if __name__ == '__main__':
    file_path = 'C:/Users/Korisnik/IdeaProjects/WorkHubConnect/data/COVID_Death_USA.csv'
    run_worker_analysis(file_path)
