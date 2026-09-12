"""Mapper process: performs the Map and Partition steps for one input split.

Usage:
    python Mapper.py <mapper_index> <sleep_seconds>

``mapper_index`` is 0-based on the command line; mapper *i* (0-based) becomes
mapper id ``i + 1`` and listens on port ``4041 + i``.

``sleep_seconds`` delays the start of every Map/Reduce handler. It exists only
to test failure Scenario 2 from the spec: the process prints
"SLEEPING... Please Terminate" and gives you a window in which to force-stop it
so the master has to reassign the task. Pass 0 for a normal run.

Two RPCs are served:

* ``Map``      -- called by the master. Reads the mapper's index range out of
                  ``Data/Input/points.txt``, assigns every point to its nearest
                  centroid, and partitions the resulting key-value pairs into
                  ``Data/Mappers/M<id>/partition_<1..R>.txt``. To simulate
                  failure Scenario 1 it answers "FAILED" with probability
                  1 - FAILURE_FREE_PROBABILITY.
* ``Reduce``   -- called by a reducer, not by the master. Streams back the
                  key-value pairs of the partitions that reducer owns, so the
                  reducer never reads the intermediate files directly.

Everything printed here is also appended to ``Data/Dump/M<id>_dump.txt``.
"""

import os
import random
import sys
import time
from concurrent import futures

import grpc

import mapreduce_pb2
import mapreduce_pb2_grpc

MASTER_PORT = 4040
DUMP_PATH = ""
INPUT_PATH = "Data/Input/points.txt"

# Probability that a Map call is reported as successful. The remaining
# probability mass is the injected Scenario 1 failure.
FAILURE_FREE_PROBABILITY = 0.95


def dump(message):
    """Append one line to this mapper's dump file."""
    with open(DUMP_PATH, "a") as file:
        file.write(message + "\n")


def calculate_min_distance(point, centroids):
    """Return the index of the centroid closest to ``point``.

    Squared Euclidean distance is enough here -- the square root would not
    change which centroid wins.
    """
    min_distance = float("inf")
    min_index = -1
    for centroid_index in range(len(centroids)):
        centroid_point_x = centroids[centroid_index].x
        centroid_point_y = centroids[centroid_index].y
        distance = (point[0] - centroid_point_x) ** 2 + (point[1] - centroid_point_y) ** 2
        if distance < min_distance:
            min_index = centroid_index
            min_distance = distance
    return min_index


def clear_directory(directory):
    """Recursively delete everything inside ``directory``, keeping the directory itself."""
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            clear_directory(file_path)
            os.rmdir(file_path)


def clear_file(file_path):
    """Truncate ``file_path``, creating it if it does not exist yet."""
    with open(file_path, "w"):
        pass


class MapReduceService(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def Map(self, request, context):
        """Master -> mapper: run map + partition over the assigned index range."""
        print(" ✉️ Recieved a Map Request!")
        dump("Received a Map Request from Master.")
        mapper_response = handle_map_request(request)

        # Injected failure Scenario 1: the task ran, but the mapper reports it
        # as failed so that the master has to retry it.
        if random.random() > FAILURE_FREE_PROBABILITY and mapper_response == "OK":
            print("❌ Mapper Failed!")
            dump("Mapper Failed like Scenario 1")
            mapper_response = "FAILED"
        if mapper_response == "OK":
            dump("Sent a Response to Master that Mapping(Map) and Partitioning is finished")
        return mapreduce_pb2.MapResponse(status=mapper_response)

    def Reduce(self, request, context):
        """Reducer -> mapper: hand over the key-value pairs of the requested partitions."""
        print(" 🔬 Recieved a Reduce Request!")
        dump("Received a Reduce Request from Reducer.")
        return handle_reduce_request(request)


def handle_reduce_request(request):
    """Read the requested partition files and group their points by centroid id."""
    centroid_values_dict_helper = {}
    for partition in request.partitions:
        with open(f"Data/Mappers/M{current_mapper_index}/partition_{partition}.txt") as file:
            points = file.readlines()
        for point in points:
            point = point.strip().split(",")
            key = int(point[0])
            data_point = [float(point[1]), float(point[2])]
            centroid_values_dict_helper.setdefault(key, []).append(data_point)

    final_values_to_send = []
    for key, points in centroid_values_dict_helper.items():
        temp_centroid_values = mapreduce_pb2.centroid_values()
        temp_centroid_values.key = key
        for point in points:
            temp_centroid_values.values.append(mapreduce_pb2.point(x=point[0], y=point[1]))
        final_values_to_send.append(temp_centroid_values)
    response = mapreduce_pb2.ReduceResponse(dictionary=final_values_to_send)
    dump("Sent a Response to the Reducer with the key value pairs")
    return response


def handle_map_request(request):
    """Map the assigned index range to (centroid id, point) pairs and partition them.

    Scenario 1 of the input-split spec: the whole input file is read here and
    only the indices ``[request.begin, request.end)`` are processed.

    A key is routed to partition ``(key % R) + 1``, which guarantees that all
    pairs sharing a key land in the same partition, i.e. reach the same reducer.
    With ``request.append`` set the existing partition files are kept, which is
    how a split reassigned from a dead mapper is merged in.
    """
    print("SLEEPING... Please Terminate")
    time.sleep(int(sys.argv[2]))
    num_reducers = request.num_reducers
    centroids = request.centroids
    append = request.append
    with open(INPUT_PATH) as file:
        points = file.readlines()

    mapper_points = []
    for index in range(request.begin, request.end):
        point = points[index].strip().split(",")
        mapper_points.append([float(point[0]), float(point[1])])

    # Map: key = nearest centroid index, value = the point itself.
    centroidIndex_to_point = {i: [] for i in range(len(centroids))}
    for point in mapper_points:
        min_distance_index = calculate_min_distance(point, centroids)
        centroidIndex_to_point[min_distance_index].append(point)

    mapper_directory = f"Data/Mappers/M{current_mapper_index}"
    if append == 0:
        if not os.path.exists(mapper_directory):
            os.makedirs(mapper_directory)
        else:
            clear_directory(mapper_directory)
        for i in range(num_reducers):
            clear_file(f"{mapper_directory}/partition_{i + 1}.txt")

    # Partition: R files per mapper, so M * R partitions in total.
    for centroid_index, points_for_centroid in centroidIndex_to_point.items():
        file_index = (centroid_index % num_reducers) + 1
        with open(f"{mapper_directory}/partition_{file_index}.txt", "a") as file:
            for point in points_for_centroid:
                file.write(f"{centroid_index},{point[0]},{point[1]}\n")
    return "OK"


if __name__ == "__main__":
    print("📒 Hello, I am am Mapper!")
    current_mapper_index = int(sys.argv[1]) + 1
    DUMP_PATH = f"Data/Dump/M{current_mapper_index}_dump.txt"
    os.makedirs(os.path.dirname(DUMP_PATH), exist_ok=True)
    current_mapper_port = MASTER_PORT + current_mapper_index
    print("Mapper Index: ", current_mapper_port)
    dump("Hello, I am a Mapper " + str(current_mapper_index))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(MapReduceService(), server)
    server.add_insecure_port("[::]:" + str(current_mapper_port))
    server.start()
    server.wait_for_termination()
