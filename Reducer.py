import grpc
import mapreduce_pb2_grpc
import mapreduce_pb2
import sys 
from concurrent import futures
import threading 
REDUCER_ID = -1
RESPONSES = []
lock = threading.Lock()

def send_reduce_request_to_mapper(mapper_port_number):

    address = f'localhost:{mapper_port_number}'
    with grpc.insecure_channel(address) as channel:
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        reduce_request = mapreduce_pb2.ReduceRequest(reducer_id=REDUCER_ID)
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
    num_of_mappers = response.num_mappers

    mapper_port_number = []
    for i in range(num_of_mappers):
        mapper_port_number.append(4040 + (i+1))
    
    threads = []
    for i in range(num_of_mappers):
        thread = threading.Thread(target=send_reduce_request_to_mapper, args=(mapper_port_number[i],))
        thread.start()
        
        threads.append(thread)
    for thread in threads:
        thread.join()

    for response in RESPONSES:
        pass 
    



class MapReduceServiceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def StartReduce(self, request, context):
        old_response = mapreduce_pb2.StartReduceResponse(ok=True)
        handle_start_reduce_request(request)
        return old_response

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


    
    
