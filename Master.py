from concurrent import futures
import sys
import os
import grpc
import time
import mapreduce_pb2_grpc
import threading
import mapreduce_pb2
import random
random.seed(37)
COLLECTIING_REDUCE_ACK = {}
MAP_FAILURE={}
lock = threading.Lock()
def compose_return_reduce_request(reducer_idx, reducer_port):
    try:
        request = mapreduce_pb2.returnReduce(ok=1)
        channel = grpc.insecure_channel(f'localhost:{reducer_port}')
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        response = stub.returnCentroid(request)
        print(f"📨 Recieved a Return Reduce Response from PORT {reducer_port}: status {response.ok}")
        if (response.ok == 0):
            print("❌ Error in Return Reduce Response retrying...")
            return compose_return_reduce_request(reducer_idx, reducer_port)
        lock.acquire()
        COLLECTIING_REDUCE_ACK[reducer_idx] = response.ok
        lock.release()
    except Exception as e:
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

    request.num_reducers = number_of_reducers
    try:
        channel = grpc.insecure_channel(f'localhost:{mapper_port}')
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        response = stub.Map(request)
        if (response.status == "FAILED"):
            print("❌ Error in Map Request retrying...")
            return compose_map_request(begin, end, centroids, mapper_port, number_of_mappers,number_of_reducers)
    except:
        next_port=4041+((mapper_port-4041)+1)%number_of_mappers
        print("FAILED TO SEND MESSAGE TO MAPPER. Redirecting to next mapper with port:",next_port)
        return compose_map_request(begin,end,centroids,next_port,number_of_mappers,number_of_reducers,append=1)

    print(f"📨 Recieved a Map Response from PORT {mapper_port}: status {response.status}")
    
def compose_reduce_request(reducer_port, number_of_mappers,number_of_reducer, partition,append=0):
    try:
        request = mapreduce_pb2.StartReduceRequest()
    except Exception as e:
        print("❌ Error in Start Reduce Request")
    
    #TODO: Add the partitions in case of reducer failure
    try:
        request.partitions.append(partition)
        request.num_mappers = number_of_mappers
        request.append=append
        channel = grpc.insecure_channel(f'localhost:{reducer_port}')
        stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
        response = stub.StartReduce(request)
        print(f"📨 Recieved a Reduce Response from PORT {reducer_port}: status {response.ok}")
    except Exception as e:
        print(e)
        all_ports=[4040-i for i in range(1,number_of_reducer+1)]
        all_ports.remove(reducer_port)
        next_port=random.sample(all_ports,1)
        print("FAILED TO SEND MESSAGE TO reducer. Redirecting to next reducer with port:",next_port[0])
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
    epsilon = 0.00000001
    flag=0
    for i in range(len(centroids)):
        a=centroids[i]
        for j in range(len(new_centroids)):
            b=new_centroids[j]
            if (abs(a[0]-b[0])<epsilon and abs(a[1]-b[1])<epsilon):
                flag+=1
                break
    if flag==len(centroids):
        return True
    else:
        return False
    

if __name__ == "__main__":
    print("🧑🏻 Hello, I am a Master")
    Number_of_mappers = int(sys.argv[1])
    Number_of_reducers = int(sys.argv[2])
    Number_of_centroids = int(sys.argv[3])
    Number_of_iterations = int(sys.argv[4])
    if (Number_of_centroids < Number_of_reducers):
        Number_of_reducers = Number_of_centroids
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

        master_port = 4040
        mapper_ports = [master_port + i for i in range(1, Number_of_mappers + 1)]
        reducer_ports = [master_port - i for i in range(1, Number_of_reducers + 1)]
        
        mapper_threads = []
        # starting mappers 
        for port_index in range(len(mapper_ports)):
            print("💌 Sending Map request to PORT",mapper_ports[port_index])
            mapper_threads.append(threading.Thread(target=compose_map_request, args=(input_to_mappers[port_index][0], input_to_mappers[port_index][1], centroids, mapper_ports[port_index],Number_of_mappers, Number_of_reducers)))
            mapper_threads[-1].start()
            
        for mapper_thread in mapper_threads:
            mapper_thread.join()
        
        reducer_threads = []
        for port_index in range(len(reducer_ports)):
            print("💌 Sending Reduce request to PORT",reducer_ports[port_index])
            reducer_threads.append(threading.Thread(target=compose_reduce_request, args=(reducer_ports[port_index], Number_of_mappers,Number_of_reducers,4040-reducer_ports[port_index])))
            reducer_threads[-1].start()

        for reducer_thread in reducer_threads:
            reducer_thread.join()


        reducer_threads = []
        new_centroids = []
        for i in range(Number_of_reducers):
            print(f"Reading from Reducer {i+1}")
            reducer_threads.append(threading.Thread(target=compose_return_reduce_request,args=(i+1,reducer_ports[i])))
            reducer_threads[-1].start()

        for reducer_thread in reducer_threads:
            reducer_thread.join()

        # post processing
        for i in COLLECTIING_REDUCE_ACK.keys():
            if COLLECTIING_REDUCE_ACK[i] == 1:
                with open(f"Data/Reducers/R{i}.txt", "r") as file:
                    for line in file:
                        new_centroids.append([float(line.split(" ")[0]), float(line.split(" ")[1])])
        # check if the new centroids and old centroids match or not
        convergence= check_convergence(centroids, new_centroids)
        if (convergence):
            print("🏁 Converged in",k+1)
            print("old centroid",centroids)
            print("new centroid",new_centroids)
            break
        else:
            print("old centroid",centroids)
            print("new centroid",new_centroids)
            centroids = new_centroids.copy()
            print("🔄 Iteration", k+1)
    
    with open("Data/centroids.txt","w") as file:
        for point in centroids:
            file.write(f"{point[0]},{point[1]}\n")





            


