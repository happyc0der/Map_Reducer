import grpc
import sys
import os
import mapreduce_pb2_grpc
import mapreduce_pb2
from concurrent import futures
import time 

current_mapper_index = -1
centroidIndex_to_point = {}

def calculate_min_distance(point, centroids):
    min_distance = float("inf")
    min_index = -1
    for centroid_index in range(len(centroids)):
        centroid_point_x = centroids[centroid_index].x
        centroid_point_y = centroids[centroid_index].y
        distance = ((point[0] - centroid_point_x) ** 2 + (point[1] - centroid_point_y) ** 2) ** 0.5
        if distance < min_distance:
            min_index = centroid_index
            min_distance = distance
    return min_index

class MapReduceService(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def Map(self, request, context):
        print(" ✉️ Recieved a Map Request!")
        mapper_response = handle_map_request(request)
        response = mapreduce_pb2.MapResponse(status=mapper_response)
        return response

    def Reduce(self, request, context):
        print(" 🔬 Recieved a StartReduce Request!")
        return handle_start_reduce_request(request)
    

def handle_start_reduce_request(request):
    centroid_values = mapreduce_pb2.ReduceResponse.dictionary
    list_of_partitions = request.partitions
    for partition in list_of_partitions:
        temp_centroid_values = mapreduce_pb2.centroid_values
        current = partition
        for centroidKey in centroidIndex_to_point.keys():
            if centroidKey == current:
                for point in centroidIndex_to_point[centroidKey]:
                    temp_point = mapreduce_pb2.point(x=point[0], y=point[1])
                    temp_centroid_values.points.append(temp_point)
                break 
        centroid_values.append(temp_centroid_values)
    response = mapreduce_pb2.ReduceResponse(dictionary=centroid_values)
    return response
                    
def handle_map_request(request):
    num_reducers = request.num_reducers
    centroids = request.centroids
    with open("Data/Input/points.txt", "r") as file:
        points = file.readlines()

    mapper_points = []

    for index in range(request.begin, request.end):
        point = points[index].strip().split(",")
        point = [float(point[0]), float(point[1])]
        mapper_points.append(point)

    centroidIndex_to_point = {}
    for point in mapper_points:
        min_distance_index = calculate_min_distance(point, request.centroids)
        if min_distance_index in centroidIndex_to_point.keys():
            centroidIndex_to_point[min_distance_index].append(point)
        else:
            centroidIndex_to_point[min_distance_index] = [point]

    # Setting up partition files
    for centroid_index in centroidIndex_to_point.keys():
        file_index = centroid_index % num_reducers + 1
        if not os.path.exists(f"Data/Mappers/M{current_mapper_index}"):
            os.makedirs(f"Data/Mappers/M{current_mapper_index}")
        with open(
            f"Data/Mappers/M{current_mapper_index}/partition_{file_index}.txt", "w+"
        ) as file:
            for point in centroidIndex_to_point[centroid_index]:
                file.write(f"{centroids[centroid_index]},{point[0]},{point[1]}\n")

    return "OK"

if __name__ == "__main__":
    print("📒 Hello, I am am Mapper!")
    current_mapper_index = int(sys.argv[1]) + 1
    current_mapper_port = 4040 + current_mapper_index
    print("Mapper Index: ", current_mapper_port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(
        MapReduceService(), server
    )
    server.add_insecure_port("[::]:" + str(current_mapper_port))
    server.start()
    server.wait_for_termination()
