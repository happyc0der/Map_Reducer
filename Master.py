from concurrent import futures
import sys
import grpc
import mapreduce_pb2_grpc
import mapreduce_pb2
import random
random.seed(42)

def Input_Split(points, Number_of_mappers):
    # gives the first len(points)/Number_of_mappers points to each mapper
    input_to_mappers = []
    for i in range(Number_of_mappers):
        input_to_mappers.append([])
    divide = len(points)//Number_of_mappers
    for i in range(Number_of_mappers):
        begin = i*divide
        end = i*divide + divide
        if i == Number_of_mappers - 1:
            end = len(points)
        input_to_mappers[i] = [str(begin),str(end)]
    # each mapper will process from begin to end - 1
    return input_to_mappers

if __name__ == "__main__":
    Number_of_mappers = int(sys.argv[1])
    Number_of_reducers = int(sys.argv[2])
    Number_of_centroids = int(sys.argv[3])
    Number_of_iterations = int(sys.argv[4])
    starting_points = []
    # read the starting points which is located within the data/input/points.txt file
    with open("data/input/points.txt", "r") as file:
        for line in file:
            starting_points.append(line.strip())
    print(starting_points)
    starting_points = [point.split(",") for point in starting_points]
    # choose k centroids without replacement fom the starting points, those will be our initial centroids
    centroids = random.sample(starting_points, Number_of_centroids)
    input_to_mappers = Input_Split(starting_points, Number_of_mappers)


    master_port = 4040
    mapper_ports = [master_port + i for i in range(1,Number_of_mappers+1)]
    reducer_ports = [master_port + Number_of_mappers + i for i in range(1,Number_of_reducers+1)]

    

    # we will be the client side of the grpc communication and the server is the mapper
    # we will be sending the input to the mappers
    try:
        with grpc.insecure_channel(f'localhost:{master_port}') as channel:
            stub = mapreduce_pb2_grpc.MasterStub(channel)
            response = stub.GetStatus(mapreduce_pb2.Empty())
            print("Master received: " + response.message)
    except:
        print("Master not running")
        sys.exit(1)


    
            




