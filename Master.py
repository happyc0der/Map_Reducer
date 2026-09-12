"""Master process: drives one K-Means run on top of the MapReduce framework.

Usage:
    python Master.py <num_mappers> <num_reducers> <num_centroids> <num_iterations>

The master owns the iteration loop. For every iteration it:

1. splits the input index range across the mappers (Scenario 1 of the spec --
   every mapper reads ``Data/Input/points.txt`` itself and only processes the
   index range the master hands it),
2. issues a ``Map`` RPC to each mapper and waits for all of them,
3. issues a ``StartReduce`` RPC to each reducer, which makes the reducers pull
   their partitions from every mapper, shuffle/sort and reduce,
4. issues a ``returnCentroid`` RPC to each reducer to collect the updated
   centroids, and compiles them into the centroid list for the next iteration,
5. stops early once the centroids stop moving by more than EPSILON.

Ports are derived from the master port 4040: mapper *i* (1-indexed) listens on
``4040 + i`` and reducer *i* on ``4040 - i``.

Everything printed here is also appended to ``Data/Dump/master_dump.txt``.
"""

import os
import random
import sys
import threading

import grpc

import mapreduce_pb2
import mapreduce_pb2_grpc

MASTER_PORT = 4040
DUMP_PATH = "Data/Dump/master_dump.txt"
INPUT_PATH = "Data/Input/points.txt"
INITIAL_CENTROIDS_PATH = "Data/initial_centroids.txt"
FINAL_CENTROIDS_PATH = "Data/centroids.txt"

# Convergence threshold: a centroid is considered unchanged when both of its
# coordinates move by less than this amount between two iterations.
EPSILON = 0.0001

# reducer index -> list of returnReduceResponse messages received from it.
# Responses accumulate across iterations; centroid compilation keys them by
# centroid id, so the newest response for a key wins.
COLLECTED_REDUCE_ACKS = {}
lock = threading.Lock()


def use_utf8_stdio():
    """Keep the log lines printable and live whatever stdout happens to be.

    Two things go wrong when this output is redirected to a file or a pipe rather
    than a terminal, and both are invisible when launching through run.py,
    because a cmd.exe window is a real console:

    * Windows defaults stdout to the ANSI code page (cp1252), which cannot encode
      the emoji in the messages below, so the very first print would raise
      UnicodeEncodeError and kill the process.
    * Python switches from line buffering to 8 KB block buffering, so a log file
      stays empty while the run is in progress, and anything still buffered is
      lost if the process is killed -- exactly what happens when testing the
      force-stop failure scenario.
    """
    for stream in (sys.stdout, sys.stderr):
        if stream is not None and hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)


def dump(message):
    """Append one line to the master's dump file."""
    with open(DUMP_PATH, "a", encoding="utf-8") as file:
        file.write(message + "\n")


def compose_return_reduce_request(reducer_idx, reducer_port):
    """Ask one reducer for its updated centroids (4th gRPC call).

    Handles both failure scenarios from the spec:

    * Scenario 1 -- the reducer answers with ``ok == 0`` (a self-reported
      failure). The same reducer is retried.
    * Scenario 2 -- the reducer process is gone, so the RPC itself raises. The
      failure is logged and this reducer contributes nothing this iteration.
    """
    try:
        request = mapreduce_pb2.returnReduce(ok=1)
        channel = grpc.insecure_channel(f"localhost:{reducer_port}")
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        response = stub.returnCentroid(request)
        print(f"📨 Recieved a Return Reduce Response from PORT {reducer_port}: status {response.ok}")
        if response.ok == 0:
            print("❌ Error in Return Reduce Response retrying...")
            dump("Reducer " + str(reducer_idx) + " Failed like Scenario 1, retrying...")
            compose_return_reduce_request(reducer_idx, reducer_port)
            return
        with lock:
            COLLECTED_REDUCE_ACKS.setdefault(reducer_idx, []).append(response)
    except Exception:
        dump("Reducer " + str(reducer_idx) + " Failed like Scenario 2")
        print("❌ Error in Return Reduce Request")


def compose_map_request(begin, end, centroids, mapper_port, number_of_mappers, number_of_reducers, append=0):
    """Send the ``Map`` RPC for the index range [begin, end) to one mapper (1st gRPC call).

    ``append`` is forwarded to the mapper: 0 makes it wipe and recreate its
    partition files, 1 makes it append to whatever is already there. It is set
    to 1 when this split is being re-run on a *different* mapper (Scenario 2),
    so the receiving mapper keeps the split it already owns.
    """
    request = mapreduce_pb2.MapRequest()
    request.begin = begin
    request.end = end
    request.append = append
    for centroid in centroids:
        point = mapreduce_pb2.point(x=centroid[0], y=centroid[1])
        request.centroids.append(point)
    mapper_id = mapper_port - MASTER_PORT
    request.num_reducers = number_of_reducers
    try:
        channel = grpc.insecure_channel(f"localhost:{mapper_port}")
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        response = stub.Map(request)
        if response.status == "FAILED":
            # Scenario 1: the mapper reported the task as failed. Retry it on
            # the same mapper.
            print("❌ Error in Map Request retrying...")
            dump("Mapper " + str(mapper_id) + " Failed like Scenario 1, retrying...")
            return compose_map_request(begin, end, centroids, mapper_port, number_of_mappers, number_of_reducers)
    except Exception:
        # Scenario 2: the mapper process is unreachable. Reassign the split to
        # the next mapper, telling it to append rather than clear its output.
        next_port = MASTER_PORT + 1 + ((mapper_port - (MASTER_PORT + 1)) + 1) % number_of_mappers
        print("FAILED TO SEND MESSAGE TO MAPPER. Redirecting to next mapper with port:", next_port)
        dump(
            "Mapper " + str(mapper_id) + " Failed like Scenario 2, redirecting to next mapper with id: "
            + str(next_port - MASTER_PORT)
        )
        return compose_map_request(begin, end, centroids, next_port, number_of_mappers, number_of_reducers, append=1)

    print(f"📨 Recieved a Map Response from ID {mapper_id}: status {response.status}")


def compose_reduce_request(reducer_port, number_of_mappers, number_of_reducer, partition, append=0):
    """Tell one reducer to start shuffling/sorting/reducing its partition (2nd gRPC call).

    If the reducer is unreachable (Scenario 2) the partition is handed to a
    randomly chosen surviving reducer with ``append=1``.
    """
    request = mapreduce_pb2.StartReduceRequest()
    reducer_id = MASTER_PORT - reducer_port
    try:
        request.partitions.append(partition)
        request.num_mappers = number_of_mappers
        request.append = append
        channel = grpc.insecure_channel(f"localhost:{reducer_port}")
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        response = stub.StartReduce(request)
        print(f"📨 Recieved a Reduce Response from PORT {reducer_port}: status {response.ok}")
        dump("Received an ack for starting reduciton from Reducer with id " + str(reducer_id) + " : SUCCESS")
    except Exception:
        all_ports = [MASTER_PORT - i for i in range(1, number_of_reducer + 1)]
        all_ports.remove(reducer_port)
        next_port = random.sample(all_ports, 1)
        print("FAILED TO SEND MESSAGE TO reducer. Redirecting to next reducer with port:", next_port[0])
        dump(
            "Reducer with id " + str(reducer_id) + " Failed like Scenario 2, redirecting to next reducer with id: "
            + str(MASTER_PORT - next_port[0])
        )
        compose_reduce_request(next_port[0], number_of_mappers, number_of_reducer, partition, append=1)


def Input_Split(points, Number_of_mappers):
    """Split the input into one contiguous ``[begin, end)`` index range per mapper.

    Only index ranges are produced -- the data itself is never shipped to the
    mappers (Scenario 1 of the spec). The last mapper absorbs the remainder of
    an uneven division.
    """
    input_to_mappers = []
    divide = len(points) // Number_of_mappers
    for i in range(Number_of_mappers):
        begin = i * divide
        end = len(points) if i == Number_of_mappers - 1 else i * divide + divide
        input_to_mappers.append([begin, end])
    return input_to_mappers


def check_convergence(centroids, new_centroids):
    """Return True when every centroid moved by less than EPSILON on both axes."""
    flag = 0
    for i in range(len(centroids)):
        a = centroids[i]
        b = new_centroids[i]
        if abs(a[0] - b[0]) < EPSILON and abs(a[1] - b[1]) < EPSILON:
            flag += 1
    return flag == len(centroids)


if __name__ == "__main__":
    Number_of_mappers = int(sys.argv[1])
    Number_of_reducers = int(sys.argv[2])
    Number_of_centroids = int(sys.argv[3])
    Number_of_iterations = int(sys.argv[4])
    # A reducer owns at least one centroid key, so more reducers than centroids
    # would just leave some of them idle.
    if Number_of_centroids < Number_of_reducers:
        Number_of_reducers = Number_of_centroids
    use_utf8_stdio()
    os.makedirs(os.path.dirname(DUMP_PATH), exist_ok=True)
    print("🧑🏻 Hello, I am a Master")
    dump("Hello, I am a Master")
    starting_points = []
    with open(INPUT_PATH) as file:
        for line in file:
            starting_points.append(line.strip())

    starting_points = [point.split(",") for point in starting_points]

    for index in range(len(starting_points)):
        starting_points[index] = [float(starting_points[index][0]), float(starting_points[index][1])]

    # Before the first iteration the centroids are sampled from the input.
    centroids = random.sample(starting_points, Number_of_centroids)

    with open(INITIAL_CENTROIDS_PATH, "w") as file:
        for centroid in centroids:
            file.write(str(centroid[0]) + "," + str(centroid[1]) + "\n")

    for k in range(Number_of_iterations):
        input_to_mappers = Input_Split(starting_points, Number_of_mappers)
        dump("Starting Iteration " + str(k + 1) + " wtih centroids " + str(centroids))
        dump("Input Split by Master Done using Scenario 1")
        mapper_ports = [MASTER_PORT + i for i in range(1, Number_of_mappers + 1)]
        reducer_ports = [MASTER_PORT - i for i in range(1, Number_of_reducers + 1)]

        # 1st gRPC call: map + partition on every mapper.
        mapper_threads = []
        dump("Sending Map Request to Mappers")
        for port_index in range(len(mapper_ports)):
            print("💌 Sending Map request to PORT", mapper_ports[port_index])
            mapper_threads.append(threading.Thread(
                target=compose_map_request,
                args=(
                    input_to_mappers[port_index][0],
                    input_to_mappers[port_index][1],
                    centroids,
                    mapper_ports[port_index],
                    Number_of_mappers,
                    Number_of_reducers,
                ),
            ))
            mapper_threads[-1].start()

        for mapper_thread in mapper_threads:
            mapper_thread.join()
        dump("Mapping Done")

        # 2nd gRPC call: reducer i is responsible for partition i, which it
        # pulls from every mapper (the 3rd gRPC call, made by the reducer).
        dump("Sending Reduce Request to all Reducers")
        reducer_threads = []
        for port_index in range(len(reducer_ports)):
            print("💌 Sending Reduce request to PORT", reducer_ports[port_index])
            reducer_threads.append(threading.Thread(
                target=compose_reduce_request,
                args=(
                    reducer_ports[port_index],
                    Number_of_mappers,
                    Number_of_reducers,
                    MASTER_PORT - reducer_ports[port_index],
                ),
            ))
            reducer_threads[-1].start()

        for reducer_thread in reducer_threads:
            reducer_thread.join()

        # 4th gRPC call: collect the updated centroids.
        dump("Asking Reducers to send the centroid data to Master")
        reducer_threads = []

        for i in range(Number_of_reducers):
            print(f"Reading from Reducer {i + 1}")
            reducer_threads.append(threading.Thread(
                target=compose_return_reduce_request,
                args=(i + 1, reducer_ports[i]),
            ))
            reducer_threads[-1].start()

        for reducer_thread in reducer_threads:
            reducer_thread.join()
        dump("Received all the centroid data from Reducers")

        # Centroid compilation: keyed by centroid id so the newest value for a
        # key overwrites anything older, then ordered by centroid id.
        new_centroids = {}
        for responses in COLLECTED_REDUCE_ACKS.values():
            for res in responses:
                if res.ok == 1:
                    for centroid in res.centroids:
                        new_centroids[centroid.key] = [centroid.x, centroid.y]
        new_centroids = [new_centroids[key] for key in sorted(new_centroids)]
        dump("Centroid Compilation is done")

        convergence = check_convergence(centroids, new_centroids)
        print("old centroid", centroids)
        print("new centroid", new_centroids)
        if convergence:
            print("🏁 Converged in", k + 1)
            dump("Converged in Iteration " + str(k + 1))
            break
        centroids = new_centroids
        print("🔄 Iteration", k + 1)

    with open(FINAL_CENTROIDS_PATH, "w") as file:
        for point in centroids:
            file.write(f"{point[0]},{point[1]}\n")
    dump("Finished writing the final centroids to the file")
