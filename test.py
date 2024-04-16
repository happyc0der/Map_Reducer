import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc

def run_map(client, data, centroids):
    request = mapreduce_pb2.MapRequest(data=data, centroids=centroids)
    try:
        response = client.Map(request)
        print("Map Response:")
        for key, value in response.results.items():
            print(f"{key}: {value}")
    except grpc.RpcError as e:
        print(f"An error occurred: {e.code()} {e.details()}")

def run_reduce(client, key, values):
    request = mapreduce_pb2.ReduceRequest(key=key, values=values)
    try:
        response = client.Reduce(request)
        print("Reduce Response:")
        print(f"Centroid ID: {response.key}, New Centroid: {response.newCentroid}")
    except grpc.RpcError as e:
        print(f"An error occurred: {e.code()} {e.details()}")

def run_start_reduce(client, flag):
    request = mapreduce_pb2.StartReduceRequest(Flag=flag)
    try:
        response = client.StartReduce(request)
        print("Start Reduce Response:")
        print(f"Operation Successful: {response.ok}")
    except grpc.RpcError as e:
        print(f"An error occurred: {e.code()} {e.details()}")

def main():
    channel = grpc.insecure_channel('localhost:4040')
    client = mapreduce_pb2_grpc.MapReduceServiceStub(channel)

    # Example data
    data = ["data1", "data2", "data3"]
    centroids = ["centroid1", "centroid2"]

    # Run Map
    run_map(client, data, centroids)

    # Run Reduce
    run_reduce(client, "centroid1", ["data1", "data3"])

    # Start Reduce
    run_start_reduce(client, True)

if __name__ == "__main__":
    main()
