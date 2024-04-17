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
            print("🔴 Recieved a Reduce Request!")
            lock.acquire()
            RESPONSES.append(response)
            lock.release()
        except:
            print("❌ Error in Reduce Request") # according to sir this error will never happen
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
        key = response.centroid_values.key 
        values = response.centroid_values.values
        if key not in Collecting_Centroid_Data:
            Collecting_Centroid_Data[key] = values
        else:
            Collecting_Centroid_Data[key].extend(values)
            
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
        final_centroids.append([key, mapreduce_pb2.Point(x=sum_x/count, y=sum_y/count)])

    



class MapReduceServiceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def StartReduce(self, request, context):
        handle_start_reduce_request(request)
        response = mapreduce_pb2.ReduceResponse(status="OK")
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


    
    
