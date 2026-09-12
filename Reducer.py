"""Reducer process: performs the Shuffle & Sort and Reduce steps for one partition.

Usage:
    python Reducer.py <reducer_index> <sleep_seconds>

``reducer_index`` is 0-based on the command line; reducer *i* (0-based) becomes
reducer id ``i + 1`` and listens on port ``4039 - i``.

``sleep_seconds`` delays the start of the StartReduce handler. It exists only to
test failure Scenario 2 from the spec: the process prints
"SLEEPING... Please Terminate" and gives you a window in which to force-stop it
so the master has to reassign the partition. Pass 0 for a normal run.

Two RPCs are served:

* ``StartReduce``    -- called by the master. Fans out a ``Reduce`` call to every
                        mapper (so the reducer never reads the intermediate
                        files itself), groups the returned values by centroid id
                        (shuffle & sort), averages each group (reduce) and
                        appends the results to ``Data/Reducers/R<id>.txt``.
* ``returnCentroid`` -- called by the master to collect the updated centroids.
                        To simulate failure Scenario 1 it answers ``ok = 0``
                        with probability 1 - FAILURE_FREE_PROBABILITY, which
                        makes the master retry.

Everything printed here is also appended to ``Data/Dump/R<id>_dump.txt``.
"""

import os
import random
import sys
import threading
import time
from concurrent import futures

import grpc

import mapreduce_pb2
import mapreduce_pb2_grpc

MASTER_PORT = 4040
REDUCER_ID = -1
DUMP_PATH = ""

# Responses gathered from the mappers for the current StartReduce call.
RESPONSES = []
lock = threading.Lock()

# Probability that returnCentroid reports success. The remaining probability
# mass is the injected Scenario 1 failure.
FAILURE_FREE_PROBABILITY = 0.95


def dump(message):
    """Append one line to this reducer's dump file."""
    with open(DUMP_PATH, "a") as file:
        file.write(message + "\n")


def send_reduce_request_to_mapper(mapper_port_number, partitions_responsible_for):
    """Pull this reducer's partitions from one mapper (3rd gRPC call)."""
    mapper_id = mapper_port_number - MASTER_PORT
    address = f"localhost:{mapper_port_number}"
    with grpc.insecure_channel(address) as channel:
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        reduce_request = mapreduce_pb2.ReduceRequest(partitions=partitions_responsible_for)
        try:
            response = stub.Reduce(reduce_request)
            print("🔴 Recieved a Reduce Response!")
            dump("Received a Reduce Response from Mapper id " + str(mapper_id))
            with lock:
                RESPONSES.append(response)
        except Exception:
            # The master only starts the reduce phase once every mapper has
            # acknowledged its Map call, so in a normal run no mapper is down
            # at this point.
            print("❌ Error in Reduce Request")
            dump("Error in sending Reduce Request to Mapper id " + str(mapper_id))


def handle_start_reduce_request(request):
    """Shuffle, sort and reduce the partitions this reducer is responsible for."""
    print("SLEEPING... Please Terminate")
    time.sleep(int(sys.argv[2]))
    global RESPONSES
    num_mappers = request.num_mappers
    responsible_for = request.partitions
    mapper_port_number = [MASTER_PORT + (i + 1) for i in range(num_mappers)]

    threads = []
    dump("Sending Reduce Request to all Mappers")
    for i in range(num_mappers):
        thread = threading.Thread(
            target=send_reduce_request_to_mapper,
            args=(mapper_port_number[i], responsible_for),
        )
        thread.daemon = True
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Shuffle & sort: collect every value that shares a centroid id, across all
    # mappers, into one group.
    Collecting_Centroid_Data = {}
    dump("Shuffling and sorting the data received from Mappers")
    for response in RESPONSES:
        for centroid_value in response.dictionary:
            key = centroid_value.key
            values_as_tuples = [(point.x, point.y) for point in centroid_value.values]
            if key not in Collecting_Centroid_Data:
                Collecting_Centroid_Data[key] = values_as_tuples
            else:
                Collecting_Centroid_Data[key].extend(values_as_tuples)
    RESPONSES = []

    # Reduce: the updated centroid of a group is the mean of its points.
    final_centroids = []
    for key, values in Collecting_Centroid_Data.items():
        sum_x = 0
        sum_y = 0
        count = 0
        for value in values:
            sum_x += value[0]
            sum_y += value[1]
            count += 1
        final_centroids.append([key, sum_x / count, sum_y / count])
    dump("Reduce Function has finished processing the data and calculated the final centroids.")

    # One line per centroid: "<centroid id> <x> <y>". Appended, so the file also
    # serves as the history of this reducer's output across iterations; the
    # master keys the values it reads back by centroid id, so the newest line
    # for a key is the one that counts.
    with open(f"./Data/Reducers/R{REDUCER_ID}.txt", "a") as f:
        for centroid in final_centroids:
            f.write(f"{centroid[0]} {centroid[1]} {centroid[2]}\n")
    print(f"Reducer {REDUCER_ID} has finished reducing and written the final centroids to file.")
    dump("Finished Reducing and written the final centroids to file")


class MapReduceServiceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def StartReduce(self, request, context):
        """Master -> reducer: run shuffle & sort plus reduce, then acknowledge."""
        print("🔴 Recieved a Start Reduce Request!")
        dump("Recieved a Start Reduce Request from Master")
        handle_start_reduce_request(request)
        print("🟢 Sent a Start Reduce Response!")
        dump("Starting the Reduction Process by sending ack to Master : SUCCESS")
        return mapreduce_pb2.StartReduceResponse(ok=1)

    def returnCentroid(self, request, context):
        """Master -> reducer: return the centroids this reducer has computed."""
        dump("Recieved a Request from Master to return the centroids.")
        my_centroids = []
        with open(f"./Data/Reducers/R{REDUCER_ID}.txt") as file:
            for line in file:
                key, x, y = line.split(" ")
                my_centroids.append([int(key), float(x), float(y)])
        send_centroids = [
            mapreduce_pb2.point_key(key=centroid[0], x=centroid[1], y=centroid[2])
            for centroid in my_centroids
        ]

        if request.ok == 1:
            print("🔴 Recieved a Return Centroid Request!")
            ack = 1
        else:
            ack = 0

        # Injected failure Scenario 1: report the transfer as failed so the
        # master has to ask again.
        if ack == 1 and random.random() > FAILURE_FREE_PROBABILITY:
            ack = 0
            dump("Failed to send the centroids to Master, Retrying...")
        if ack == 1:
            dump("Sent the centroids to Master")
        return mapreduce_pb2.returnReduceResponse(ok=ack, centroids=send_centroids)


if __name__ == "__main__":
    reducer_index = int(sys.argv[1]) + 1  # 1-indexed
    reducer_port_number = MASTER_PORT - reducer_index
    REDUCER_ID = reducer_index
    DUMP_PATH = f"./Data/Dump/R{REDUCER_ID}_dump.txt"
    os.makedirs(os.path.dirname(DUMP_PATH), exist_ok=True)
    os.makedirs("./Data/Reducers", exist_ok=True)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(MapReduceServiceServicer(), server)
    server.add_insecure_port(f"[::]:{reducer_port_number}")
    server.start()
    print(f"Reducer {reducer_index} started on port {reducer_port_number}.")
    server.wait_for_termination()
