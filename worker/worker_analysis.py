import grpc
import task_pb2
import task_pb2_grpc
import pandas as pd
import os
from concurrent import futures
from io import BytesIO

def print_csv_content(file_path):
    try:
        df = pd.read_csv(file_path)
        print("Contents of the CSV file:")
        print(df.head()) 
    except Exception as e:
        print(f"Error reading CSV file: {e}")

class Worker(task_pb2_grpc.WorkerServicer):
    def AnalyzeData(self, request, context):
        data = request.data
        try:
            df = pd.read_csv(BytesIO(data))
        except Exception as e:
            context.set_details(f'Failed to read CSV data: {str(e)}')
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return task_pb2.AnalysisResponse(result="Error in analysis")
        
        print("First few rows of the data:")
        print(df.head())

        try:
            summary = df.groupby('State')['Deaths involving COVID-19'].sum()
        except KeyError as e:
            context.set_details(f'Failed to group data: {str(e)}')
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return task_pb2.AnalysisResponse(result="Error in analysis")

        print("Summary of deaths involving COVID-19 by state:")
        print(summary)

        return task_pb2.AnalysisResponse(result="Analysis complete")

def serve():
    csv_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'COVID_Death_USA.csv')
    
    print_csv_content(csv_file)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_pb2_grpc.add_WorkerServicer_to_server(Worker(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Worker server started on port 50051.")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
