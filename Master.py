from concurrent import futures
import sys
import os
import grpc
import time
import mapreduce_pb2_grpc
import threading
import mapreduce_pb2
import random

COLLECTIING_REDUCE_ACK = {}
MAP_FAILURE={}
lock = threading.Lock()
DUMP_PATH = "Data/Dump/master_dump.txt"

def dump(message):
    global DUMP_PATH
    with open(DUMP_PATH, "a") as file:
        file.write(message + "\n")

def compose_return_reduce_request(reducer_idx, reducer_port):
    try:
        request = mapreduce_pb2.returnReduce(ok=1)
        channel = grpc.insecure_channel(f'localhost:{reducer_port}')
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        response = stub.returnCentroid(request)
        print(f"📨 Recieved a Return Reduce Response from PORT {reducer_port}: status {response.ok}")
        if (response.ok == 0):
            print("❌ Error in Return Reduce Response retrying...")
            dump("Reducer " + str(reducer_idx)+ " Failed like Scenario 1, retrying...")
            return compose_return_reduce_request(reducer_idx, reducer_port)
        lock.acquire()
        if (reducer_idx not in COLLECTIING_REDUCE_ACK.keys()):
            COLLECTIING_REDUCE_ACK[reducer_idx] = [response]
        else:
            COLLECTIING_REDUCE_ACK[reducer_idx].append(response)
        lock.release()
    except Exception as e:
        dump("Reducer " + str(reducer_idx)+ " Failed like Scenario 2")
        print("❌ Error in Return Reduce Request")
    return request

def compose_map_request(begin, end, centroids, mapper_port,number_of_mappers,number_of_reducers,append=0):
    request = mapreduce_pb2.MapRequest()
    request.begin = begin
    request.end = end
    request.append=append
    for centroid in centroids:
        point = mapreduce_pb2.point(x=centroid[0], y=centroid[1])
        request.centroids.append(point)
    mapper_id = mapper_port - 4040
    request.num_reducers = number_of_reducers
    try:
        channel = grpc.insecure_channel(f'localhost:{mapper_port}')
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        response = stub.Map(request)
        if (response.status == "FAILED"):
            print("❌ Error in Map Request retrying...")
            dump("Mapper " + str(mapper_id)+ " Failed like Scenario 1, retrying...")
            return compose_map_request(begin, end, centroids, mapper_port, number_of_mappers,number_of_reducers)
    except:
        next_port=4041+((mapper_port-4041)+1)%number_of_mappers
        print("FAILED TO SEND MESSAGE TO MAPPER. Redirecting to next mapper with port:",next_port)
        dump("Mapper " + str(mapper_id) + " Failed like Scenario 2, redirecting to next mapper with id: "+str(next_port-4040))
        return compose_map_request(begin,end,centroids,next_port,number_of_mappers,number_of_reducers,append=1)

    print(f"📨 Recieved a Map Response from ID {mapper_id}: status {response.status}")
    
def compose_reduce_request(reducer_port, number_of_mappers,number_of_reducer, partition,append=0):
    try:
        request = mapreduce_pb2.StartReduceRequest()
    except Exception as e:
        print("❌ Error in Start Reduce Request")
  
    try:
        request.partitions.append(partition)
        request.num_mappers = number_of_mappers
        request.append=append
        channel = grpc.insecure_channel(f'localhost:{reducer_port}')
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        response = stub.StartReduce(request)
        print(f"📨 Recieved a Reduce Response from PORT {reducer_port}: status {response.ok}")
        dump("Received an ack for starting reduciton from Reducer with id "+str(4040-reducer_port)+ " : SUCCESS")
    except Exception as e:
        all_ports=[4040-i for i in range(1,number_of_reducer+1)]
        all_ports.remove(reducer_port)
        next_port=random.sample(all_ports,1)
        print("FAILED TO SEND MESSAGE TO reducer. Redirecting to next reducer with port:",next_port[0])
        dump("Reducer with id "+str(4040-reducer_port)+ " Failed like Scenario 2, redirecting to next reducer with id: "+str(next_port[0]-4040))
        partition=4040-next_port[0]
        return compose_reduce_request(next_port[0],number_of_mappers,number_of_reducer,partition,append=1)
        
    return


def Input_Split(points, Number_of_mappers):
    input_to_mappers = []
    for i in range(Number_of_mappers):
        input_to_mappers.append([])
    divide = len(points) // Number_of_mappers
    for i in range(Number_of_mappers):
        begin = i * divide
        end = i * divide + divide
        if i == Number_of_mappers - 1:
            end = len(points)
        input_to_mappers[i] = [begin, end]
    return input_to_mappers

def check_convergence(centroids, new_centroids):
    epsilon = 0.0001
    flag=0
    for i in range(len(centroids)):
        a=centroids[i]
        b=new_centroids[i]
        
        if abs(a[0]-b[0])<epsilon and abs(a[1]-b[1])<epsilon:
            flag+=1
    if flag==len(centroids):
        return True
    else:
        return False
    

if __name__ == "__main__":
    Number_of_mappers = int(sys.argv[1])
    Number_of_reducers = int(sys.argv[2])
    Number_of_centroids = int(sys.argv[3])
    Number_of_iterations = int(sys.argv[4])
    if (Number_of_centroids < Number_of_reducers):
        Number_of_reducers = Number_of_centroids
    print("🧑🏻 Hello, I am a Master")
    dump("Hello, I am a Master")
    starting_points = []
    with open("Data/input/points.txt", "r") as file:
        for line in file:
            starting_points.append(line.strip())

    starting_points = [point.split(",") for point in starting_points]
    
    for index in range(len(starting_points)):
        starting_points[index] = [float(starting_points[index][0]), float(starting_points[index][1])]
        
    centroids = random.sample(starting_points, Number_of_centroids)
    
    #write to Initial_centroids.txt
    with open("Data/initial_centroids.txt", "w") as file:
        for centroid in centroids:
            file.write(str(centroid[0]) + "," + str(centroid[1]) + "\n")
        
    for k in range(Number_of_iterations):
        input_to_mappers = Input_Split(starting_points, Number_of_mappers)
        dump("Starting Iteration " + str(k+1))
        dump("Input Split by Master Done using Scenario 1")
        master_port = 4040
        mapper_ports = [master_port + i for i in range(1, Number_of_mappers + 1)]
        reducer_ports = [master_port - i for i in range(1, Number_of_reducers + 1)]
        print(centroids)
        mapper_threads = []
        # starting mappers 
        dump("Sending Map Request to Mappers")
        for port_index in range(len(mapper_ports)):
            print("💌 Sending Map request to PORT",mapper_ports[port_index])
            mapper_threads.append(threading.Thread(target=compose_map_request, args=(input_to_mappers[port_index][0], input_to_mappers[port_index][1], centroids, mapper_ports[port_index],Number_of_mappers, Number_of_reducers)))
            mapper_threads[-1].start()
            
        for mapper_thread in mapper_threads:
            mapper_thread.join()
        dump("Mapping Done")

        dump("Sending Reduce Request to all Reducers")
        reducer_threads = []
        for port_index in range(len(reducer_ports)):
            print("💌 Sending Reduce request to PORT",reducer_ports[port_index])
            reducer_threads.append(threading.Thread(target=compose_reduce_request, args=(reducer_ports[port_index], Number_of_mappers,Number_of_reducers,4040-reducer_ports[port_index])))
            reducer_threads[-1].start()

        for reducer_thread in reducer_threads:
            reducer_thread.join()

        dump("Asking Reducers to send the centroid data to Master")
        reducer_threads = []
        
        for i in range(Number_of_reducers):
            print(f"Reading from Reducer {i+1}")
            reducer_threads.append(threading.Thread(target=compose_return_reduce_request,args=(i+1,reducer_ports[i])))
            reducer_threads[-1].start()

        for reducer_thread in reducer_threads:
            reducer_thread.join()
        dump("Received all the centroid data from Reducers")
        # post processing
        new_centroids = {}
        for i in COLLECTIING_REDUCE_ACK.keys():
            response = COLLECTIING_REDUCE_ACK[i]
            for res in response:
                if (res.ok == 1):
                    for centroid in res.centroids:
                        new_centroids[centroid.key] = [centroid.x, centroid.y]
        temp_centroids = []
        for key in sorted(new_centroids.keys()):
            temp_centroids.append([new_centroids[key][0], new_centroids[key][1]])
        new_centroids = temp_centroids
        dump("Centroid Compilation is done")
        # check if the new centroids and old centroids match or not
        convergence= check_convergence(centroids, new_centroids)
        if (convergence):
            print("🏁 Converged in",k+1)
            print("old centroid",centroids)
            print("new centroid",new_centroids)
            dump("Converged in Iteration " + str(k+1))
            break
        else:
            print("old centroid",centroids)
            print("new centroid",new_centroids)
            centroids = new_centroids
            print("🔄 Iteration", k+1)
    
    
    with open("Data/centroids.txt","w") as file:
        for point in centroids:
            file.write(f"{point[0]},{point[1]}\n")
    dump("Finished writing the final centroids to the file")





            


