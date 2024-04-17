from concurrent import futures
import sys
import os
import grpc
import time
import mapreduce_pb2_grpc
import threading
import mapreduce_pb2
import random

random.seed(42)

def compose_map_request(begin, end, centroids, mapper_port, number_of_reducers):
    request = mapreduce_pb2.MapRequest()
    request.begin = begin
    request.end = end
    for centroid in centroids:
        point = mapreduce_pb2.point(x=centroid[0], y=centroid[1])
        request.centroids.append(point)

    request.num_reducers = number_of_reducers
    
    channel = grpc.insecure_channel(f'localhost:{mapper_port}')
    stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
    response = stub.Map(request)
    print(f"📨 Recieved a Map Response from PORT {mapper_port}: status {response.status}")
    
def compose_reduce_request(reducer_port, number_of_mappers):
    request = mapreduce_pb2.StartReduceRequest()
    request.num_mappers = number_of_mappers
    partition = 4040 - reducer_port 
    
    #TODO: Add the partitions in case of reducer failure
    request.partitions.append(partition)
    
    return request


def Input_Split(points, Number_of_mappers):
    input_to_mappers = []
    for i in range(Number_of_mappers):
        input_to_mappers.append([])
    divide = len(points) // Number_of_mappers
    for i in range(Number_of_mappers):
        begin = i * divide
        end = i * divide + divide
        if i == Number_of_mappers - 1:
            end = len(points)
        input_to_mappers[i] = [begin, end]
    return input_to_mappers


if __name__ == "__main__":
    print("🧑🏻 Hello, I am a Master")
    Number_of_mappers = int(sys.argv[1])
    Number_of_reducers = int(sys.argv[2])
    Number_of_centroids = int(sys.argv[3])
    Number_of_iterations = int(sys.argv[4])
    starting_points = []
    with open("Data/input/points.txt", "r") as file:
        for line in file:
            starting_points.append(line.strip())

    starting_points = [point.split(",") for point in starting_points]
    
    for index in range(len(starting_points)):
        starting_points[index] = [float(starting_points[index][0]), float(starting_points[index][1])]
        
    centroids = random.sample(starting_points, Number_of_centroids)
    print(type(centroids))
    
    #write to Initial_centroids.txt
    with open("Data/initial_centroids.txt", "w") as file:
        for centroid in centroids:
            file.write(str(centroid[0]) + "," + str(centroid[1]) + "\n")
        

    input_to_mappers = Input_Split(starting_points, Number_of_mappers)

    master_port = 4040
    mapper_ports = [master_port + i for i in range(1, Number_of_mappers + 1)]
    reducer_ports = [master_port - i for i in range(1, Number_of_reducers + 1)]
    
    mapper_threads = []
    # starting mappers 
    for port_index in range(len(mapper_ports)):
        print("💌 Sending Map request to PORT",mapper_ports[port_index])
        mapper_threads.append(threading.Thread(target=compose_map_request, args=(input_to_mappers[port_index][0], input_to_mappers[port_index][1], centroids, mapper_ports[port_index], Number_of_reducers)))
        mapper_threads[-1].start()
        
    for mapper_thread in mapper_threads:
        mapper_thread.join()
    
    reducer_threads = []
    
    for port_index in range(len(reducer_ports)):
        print("💌 Sending Reduce request to PORT",reducer_ports[port_index])
        reducer_threads.append(threading.Thread(target=compose_reduce_request, args=(reducer_ports[port_index], Number_of_mappers)))
        reducer_threads[-1].start()

