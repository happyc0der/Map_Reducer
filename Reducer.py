import grpc
import mapreduce_pb2_grpc
import mapreduce_pb2
import sys 
from concurrent import futures
import threading 
REDUCER_ID = -1
RESPONSES = []
NUM_MAPPERS = -1
lock = threading.Lock()

def send_reduce_request_to_mapper(mapper_port_number, partitions_responsible_for):
    address = f'localhost:{mapper_port_number}'
    with grpc.insecure_channel(address) as channel:
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        reduce_request = mapreduce_pb2.ReduceRequest(partitions=partitions_responsible_for)
        try:
            response = stub.Reduce(reduce_request)
            print("🔴 Recieved a Reduce Response!")
            lock.acquire()
            RESPONSES.append(response)
            lock.release()
        except Exception as e:
            print("❌ Error in Reduce Request") # according to sir this error will never happen
            print(e)
    return 

def handle_start_reduce_request(response):
    NUM_MAPPERS = response.num_mappers
    responsible_for = response.partitions 

    mapper_port_number = []
    for i in range(NUM_MAPPERS):
        mapper_port_number.append(4040 + (i+1))
    
    threads = []
    for i in range(NUM_MAPPERS):
        thread = threading.Thread(target=send_reduce_request_to_mapper, args=(mapper_port_number[i],responsible_for))
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()
    Collecting_Centroid_Data = {}

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

    print(Collecting_Centroid_Data)
    final_centroids = []
    # NOT SURE THIS WORKS OR NOT
    for key in Collecting_Centroid_Data:
        sum_x = 0
        sum_y = 0
        count = 0
        for values in Collecting_Centroid_Data[key]:
            sum_x += values.x
            sum_y += values.y
            count += 1
        final_centroids.append([sum_x/count, sum_y/count]) 
    # print the final list of centroids line wise into the file at ./Data/Reducers/R{REDUCER_ID}.txt
    
    with open(f"./Data/Reducers/R{REDUCER_ID}.txt", "w") as f:
        for centroid in final_centroids:
            f.write(f"{centroid[0]} {centroid[1]}\n")
    print(f"Reducer {REDUCER_ID} has finished reducing and written the final centroids to file.")
    return 
    




class MapReduceServiceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def StartReduce(self, request, context):
        print("🔴 Recieved a Start Reduce Request!")
        handle_start_reduce_request(request)
        # compose a reply with ok as 1
        print("🟢 Sent a Start Reduce Response!")
        response = mapreduce_pb2.StartReduceResponse(ok=1)
        return response
    
    def returnCentroid(self,request,context):
        #NOTE: Can be used for fault tolerance handling if needed based on ok value
        ack = -1
        if request.ok==1:
            print("🔴 Recieved a Return Centroid Request!")
            ack = 1
        else:
            ack = 0
        response = mapreduce_pb2.returnReduceResponse(ok=ack)
        return response

    

if __name__ == "__main__":
    reducer_index = int(sys.argv[1]) + 1 # 1 - indexed
    reducer_port_number = 4040 - reducer_index
    REDUCER_ID = reducer_index
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(MapReduceServiceServicer(), server)
    server.add_insecure_port(f'[::]:{reducer_port_number}')
    server.start()
    print(f"Reducer {reducer_index} started on port {reducer_port_number}.")
    server.wait_for_termination()


    
    
