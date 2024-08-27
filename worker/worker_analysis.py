import os
import sys
import grpc
import pandas as pd
from concurrent import futures
from io import BytesIO
import csv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../protos')))
import task_pb2
import task_pb2_grpc
from grpc_reflection.v1alpha import reflection

def print_csv_content(file_path):
    try:
        df = pd.read_csv(file_path)
        print("Contents of the CSV file:")
        print(df)  
    except Exception as e:
        print(f"Error reading CSV file: {e}")

def save_analysis_to_csv(total_deaths, max_deaths_state, max_deaths_value, proportions, output_file='analysis_results.csv'):
    """Saves the analysis results to a CSV file."""
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Metric", "Value"]) 
        writer.writerow(["Total deaths involving COVID-19", total_deaths])
        writer.writerow(["State with the highest deaths", f"{max_deaths_state} ({max_deaths_value} deaths)"])
        writer.writerow(["Proportions by state"])
        for state, proportion in proportions.items():
            writer.writerow([state, proportion])
    print(f"Analysis results saved to {output_file}")

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
            print("Summary of deaths involving COVID-19 by state:")
            print(summary)

            total_deaths = df['Deaths involving COVID-19'].sum()
            print(f"Total deaths involving COVID-19: {total_deaths}")

            proportion = (summary / total_deaths) * 100
            print("Proportion of deaths by state:")
            print(proportion)

            max_deaths_state = summary.idxmax()
            max_deaths_value = summary.max()
            print(f"State with the highest deaths: {max_deaths_state} ({max_deaths_value} deaths)")

            analysis_result = (
                f"Total deaths involving COVID-19: {total_deaths}\n"
                f"State with the highest deaths: {max_deaths_state} ({max_deaths_value} deaths)\n"
                f"Proportions: {proportion.to_dict()}"
            )
            print("Analysis Results:")
            print(analysis_result)

            save_analysis_to_csv(total_deaths, max_deaths_state, max_deaths_value, proportion.to_dict())

        except KeyError as e:
            context.set_details(f'Failed to analyze data: {str(e)}')
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return task_pb2.AnalysisResponse(result="Error in analysis")

        return task_pb2.AnalysisResponse(result=analysis_result)

def serve():
    csv_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'COVID_Death_USA.csv')
    print_csv_content(csv_file)  
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_pb2_grpc.add_WorkerServicer_to_server(Worker(), server)
    
    SERVICE_NAMES = (
        task_pb2.DESCRIPTOR.services_by_name['Worker'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port('[::]:50052')
    server.start()
    print("Worker server started on port 50052.")
    
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
