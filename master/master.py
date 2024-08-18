import grpc
import os
import sys

# Dodajte putanju za uvoz task_pb2 i task_pb2_grpc
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import task_pb2
import task_pb2_grpc

class Master:
    def __init__(self):
        # Postavi gRPC kanal i stub
        self.channel = grpc.insecure_channel('localhost:50051')
        self.stub = task_pb2_grpc.WorkerStub(self.channel)

    def distribute_task(self, csv_file):
        # Ovdje šaljemo zadatak Workeru
        with open(csv_file, 'rb') as f:
            data = f.read()
        
        # Kreiraj zahtjev za analizu podataka
        request = task_pb2.AnalysisRequest(data=data)
        
        try:
            # Pošaljite zahtjev Workeru
            response = self.stub.AnalyzeData(request)
            print(f"Response from worker: {response.result}")
        except grpc.RpcError as e:
            print(f"gRPC error occurred: {e}")

if __name__ == '__main__':
    # Kreiraj instancu Mastera
    master = Master()
    
    # Definiraj relativnu putanju do CSV datoteke
    csv_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'COVID_Death_USA.csv')
    
    # Distribuiraj zadatak
    master.distribute_task(csv_file)
