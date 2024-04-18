import grpc
import sys
import os
import mapreduce_pb2_grpc
import mapreduce_pb2
from concurrent import futures
import time 
import random

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

def clear_directory(directory):
    # List all files and subdirectories in the given directory
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        # Check if the path is a file
        if os.path.isfile(file_path):
            # Remove the file
            os.remove(file_path)
        # If it's a directory, recursively clear it
        elif os.path.isdir(file_path):
            clear_directory(file_path)
            # After clearing the subdirectory, remove it
            os.rmdir(file_path)

def clear_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'w'):  # 'w' mode truncates the file
            pass  # This line does nothing, but the file is cleared
    else:
        with open(file_path, 'w'):  # 'w' mode creates a new empty file
            pass

class MapReduceService(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def Map(self, request, context):
        print(" ✉️ Recieved a Map Request!")
        mapper_response = handle_map_request(request)
        
        p = 0.5
        if random.random() > p and mapper_response == "OK":
            print("❌ Mapper Failed!")
            mapper_response = "FAILED"
        response = mapreduce_pb2.MapResponse(status=mapper_response)
        return response

    def Reduce(self, request, context):
        print(" 🔬 Recieved a Reduce Request!")
        return handle_reduce_request(request)
    

def handle_reduce_request(request):
    # This will hold the list of centroid_values messages
    centroid_values_dict_helper = {}
    list_of_partitions = request.partitions
    for partition in list_of_partitions:
        with open(f"Data/Mappers/M{current_mapper_index}/partition_{partition}.txt", "r") as file:
            points = file.readlines()
        for point in points:
            point = point.strip().split(",")
            key = int(point[0])
            data_point = [float(point[1]), float(point[2])]
            if key in centroid_values_dict_helper.keys():
                centroid_values_dict_helper[key].append(data_point)
            else:
                centroid_values_dict_helper[key] = [data_point]
    final_values_to_send = []
    for key in centroid_values_dict_helper.keys():
        temp_centroid_values = mapreduce_pb2.centroid_values()
        temp_centroid_values.key = key
        for point in centroid_values_dict_helper[key]:
            temp_point = mapreduce_pb2.point(x=point[0], y=point[1])
            temp_centroid_values.values.append(temp_point)
        final_values_to_send.append(temp_centroid_values)
    response = mapreduce_pb2.ReduceResponse(dictionary=final_values_to_send)
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
    centroidIndex_to_point={}

    for i in range(len(centroids)):
        centroidIndex_to_point[i]=[]
    for point in mapper_points:
        min_distance_index = calculate_min_distance(point, centroids)
        if min_distance_index in centroidIndex_to_point.keys():
            centroidIndex_to_point[min_distance_index].append(point)
        else:
            centroidIndex_to_point[min_distance_index] = [point]
    if not os.path.exists(f"Data/Mappers/M{current_mapper_index}"):
        os.makedirs(f"Data/Mappers/M{current_mapper_index}")
    else:
        clear_directory(f"Data/Mappers/M{current_mapper_index}")
        
    for i in range(num_reducers):
        clear_file(f"Data/Mappers/M{current_mapper_index}/partition_{i+1}.txt")
    print(centroidIndex_to_point[2])
    for centroid_index in centroidIndex_to_point.keys():
        file_index = centroid_index % num_reducers + 1
        with open(f"Data/Mappers/M{current_mapper_index}/partition_{file_index}.txt", "a") as file:
            for point in centroidIndex_to_point[centroid_index]:
                file.write(f"{centroid_index},{point[0]},{point[1]}\n")

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
