import grpc
import task_pb2
import task_pb2_grpc

def run():
    with grpc.insecure_channel('localhost:50052') as channel:
        stub = task_pb2_grpc.WorkerStub(channel)
        try:
            with open('C:/Users/Korisnik/IdeaProjects/WorkHubConnect/data/COVID_Death_USA.csv', 'rb') as f:
                data = f.read()

            response = stub.AnalyzeData(task_pb2.AnalysisRequest(data=data))
            print("Worker response: " + response.result)
        except FileNotFoundError:
            print("CSV file not found. Please check the path.")
        except grpc.RpcError as e:
            print(f"gRPC error: {e.code()} - {e.details()}")

if __name__ == '__main__':
    run()
