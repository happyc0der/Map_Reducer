import subprocess
import sys

virtualenv_python = r'python.exe'
master_script_path = r'./Master.py'
mapper_script_path = r'./Mapper.py'
reducer_script_path = r'./Reducer.py'

def run_process(script_path, *args):
    cmd = ['start', 'cmd.exe', '/k', virtualenv_python, script_path] + list(map(str, args))
    print(cmd)
    return subprocess.Popen(cmd, shell=True)

def run_mapper(num_mappers):
    return [run_process(mapper_script_path, i) for i in range(num_mappers)]

def run_reducer(num_reducers):
    return [run_process(reducer_script_path, i) for i in range(num_reducers)]

def run_master(num_mappers, num_reducers, num_centroids, num_iterations):
    return run_process(master_script_path, num_mappers, num_reducers, num_centroids, num_iterations)

def terminate_processes(processes):
    for process in processes:
        process.terminate()  # Politely ask the process to terminate
        try:
            process.wait(timeout=2)  # Wait a bit for the process to terminate
        except subprocess.TimeoutExpired:
            process.kill()  # Forcefully kill the process if it doesn't terminate

def main(num_mappers, num_reducers, num_centroids, num_iterations, sleep_time):
    
    mappers = run_mapper(num_mappers)
    reducers = run_reducer(num_reducers)
    master = run_master(num_mappers, num_reducers, num_centroids, num_iterations)

    # You might want to add some logic to wait for the processes to finish
    # or some other coordination mechanism, depending on your use case.

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: run.py <num_mappers> <num_reducers> <num_centroids> <num_iterations>")
        sys.exit(1)


    num_mappers = int(sys.argv[1])
    num_reducers = int(sys.argv[2])
    num_centroids = int(sys.argv[3])
    num_iterations = int(sys.argv[4])
    sleep_time = int(sys.argv[5])
    print("Number of Mappers: ", num_mappers)
    print("Number of Reducers: ", num_reducers)
    print("Number of Centroids: ", num_centroids)
    print("Number of Iterations: ", num_iterations)
    print("Sleep Time: ", sleep_time)
    main(num_mappers, num_reducers, num_centroids, num_iterations, sleep_time)
