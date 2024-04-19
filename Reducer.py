import grpc
import mapreduce_pb2_grpc
import mapreduce_pb2
import sys 
from concurrent import futures
import threading 
import os
import random
import time
REDUCER_ID = -1
RESPONSES = []
NUM_MAPPERS = -1
lock = threading.Lock()
DUMP_PATH = ""

def dump(message):
    global DUMP_PATH
    with open(DUMP_PATH, "a") as file:
        file.write(message + "\n")

def send_reduce_request_to_mapper(mapper_port_number, partitions_responsible_for):
    mapper_id = mapper_port_number - 4040
    address = f'localhost:{mapper_port_number}'
    with grpc.insecure_channel(address) as channel:
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        reduce_request = mapreduce_pb2.ReduceRequest(partitions=partitions_responsible_for)
        try:
            response = stub.Reduce(reduce_request)
            print("🔴 Recieved a Reduce Response!")
            dump("Received a Reduce Response from Mapper id "+str(mapper_id))
            lock.acquire()
            RESPONSES.append(response)
            lock.release()
        except Exception as e:
            print("❌ Error in Reduce Request") # according to sir this error will never happen
            dump("Error in sending Reduce Request to Mapper id "+str(mapper_id))
    return 

def handle_start_reduce_request(response):
    print("SLEEPING... Please Terminate")
    time.sleep(int(sys.argv[2]))
    global RESPONSES
    NUM_MAPPERS = response.num_mappers
    responsible_for = response.partitions 
    # append=response.append
    mapper_port_number = []
    for i in range(NUM_MAPPERS):
        mapper_port_number.append(4040 + (i+1))
    
    threads = []
    dump("Sending Reduce Request to all Mappers")
    for i in range(NUM_MAPPERS):
        thread = threading.Thread(target=send_reduce_request_to_mapper, args=(mapper_port_number[i],responsible_for))
        thread.daemon=True
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    Collecting_Centroid_Data = {}
    dump("Shuffling and sorting the data received from Mappers")
    for response in RESPONSES:
        for centroid_value in response.dictionary:
            key = centroid_value.key
            values = centroid_value.values  # This should be a list of `point` messages.
            
            # Convert each `point` message to a tuple (x, y), if needed
            values_as_tuples = [(point.x, point.y) for point in values]

            if key not in Collecting_Centroid_Data:
                Collecting_Centroid_Data[key] = values_as_tuples
            else:
                Collecting_Centroid_Data[key].extend(values_as_tuples)
    RESPONSES=[]
    final_centroids = []
    for key in Collecting_Centroid_Data.keys():
        sum_x = 0
        sum_y = 0
        count = 0
        for values in Collecting_Centroid_Data[key]:
            sum_x += values[0]
            sum_y += values[1]
            count += 1
        final_centroids.append([key,sum_x/count, sum_y/count]) 
    dump("Reduce Function has finished processing the data and calculated the final centroids.")
    # print the final list of centroids line wise into the file at ./Data/Reducers/R{REDUCER_ID}.txt
    write_flag='a'
    with open(f"./Data/Reducers/R{REDUCER_ID}.txt", write_flag) as f:
        for centroid in final_centroids:
            f.write(f"{centroid[0]} {centroid[1]} {centroid[2]}\n")
    print(f"Reducer {REDUCER_ID} has finished reducing and written the final centroids to file.")
    dump("Finished Reducing and written the final centroids to file")
    return 
    




class MapReduceServiceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def StartReduce(self, request, context):
        print("🔴 Recieved a Start Reduce Request!")
        dump("Recieved a Start Reduce Request from Master")
        handle_start_reduce_request(request)
        # compose a reply with ok as 1
        print("🟢 Sent a Start Reduce Response!")
    
        response = mapreduce_pb2.StartReduceResponse(ok=1)
        dump("Starting the Reduction Process by sending ack to Master : SUCCESS")
        return response
    
    def returnCentroid(self,request,context):
        dump("Recieved a Request from Master to return the centroids.")
        my_centroids = []
        with open(f"./Data/Reducers/R{REDUCER_ID}.txt", "r") as file:
            for line in file:
                my_centroids.append([int(line.split(" ")[0]), float(line.split(" ")[1]),float(line.split(" ")[2])])
        # clear the file
        send_centroids = [mapreduce_pb2.point_key(key=centroid[0],x=centroid[1],y=centroid[2]) for centroid in my_centroids]
        ack = -1
        if request.ok==1:
            print("🔴 Recieved a Return Centroid Request!")
            ack = 1
        else:
            ack = 0
        # with probability 0.8 set ok to 0 
        # with probability 0.2 set ok to 1
        p = 0.95
        if ack == 1 and random.random()  > p:
            ack = 0
            dump("Failed to send the centroids to Master, Retrying...")
        if (ack == 1):
            dump("Sent the centroids to Master")
        response = mapreduce_pb2.returnReduceResponse(ok=ack,centroids= send_centroids)
        return response

    

if __name__ == "__main__":
    reducer_index = int(sys.argv[1]) + 1 # 1 - indexed
    reducer_port_number = 4040 - reducer_index
    REDUCER_ID = reducer_index
    DUMP_PATH = f"./Data/Dump/R{REDUCER_ID}_dump.txt"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(MapReduceServiceServicer(), server)
    server.add_insecure_port(f'[::]:{reducer_port_number}')
    server.start()
    print(f"Reducer {reducer_index} started on port {reducer_port_number}.")
    server.wait_for_termination()


    
    
