from concurrent import futures
import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc

# Service implementation
class MapReduceServiceServicer(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def Map(self, request, context):
        # Implement your mapping logic here
        results = {str(i): f"processed_{data}" for i, data in enumerate(request.data)}
        return mapreduce_pb2.MapResponse(results=results)

    def Reduce(self, request, context):
        # Implement your reduction logic here
        new_centroid = f"centroid_of_{request.key}"
        return mapreduce_pb2.ReduceResponse(key=request.key, newCentroid=new_centroid)

    def StartReduce(self, request, context):
        # Example logic for starting a reduce task
        return mapreduce_pb2.StartReduceResponse(ok=True)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(MapReduceServiceServicer(), server)
    server.add_insecure_port('[::]:4040')
    server.start()
    print("Server started on port 4040.")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
