import subprocess
import sys
import os 
virtualenv_python = r'python.exe'
master_script_path = r'./Master.py'
mapper_script_path = r'./Mapper.py'
reducer_script_path = r'./Reducer.py'

def run_process(script_path, *args):
    cmd = ['start', 'cmd.exe', '/k', virtualenv_python, script_path] + list(map(str, args))
    print(cmd)
    return subprocess.Popen(cmd, shell=True)

def run_mapper(num_mappers,sleep_time):
    return [run_process(mapper_script_path, i,sleep_time) for i in range(num_mappers)]

def run_reducer(num_reducers,sleep_time):
    return [run_process(reducer_script_path, i,sleep_time) for i in range(num_reducers)]

def run_master(num_mappers, num_reducers, num_centroids, num_iterations):
    return run_process(master_script_path, num_mappers, num_reducers, num_centroids, num_iterations)

def clear_directory(directory):
    # List all files and subdirectories in the given directory
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        # Check if the path is a file
        if os.path.isfile(file_path):
            # Remove the file
            os.remove(file_path)
        # If it's a directory, recursively clear it
        elif os.path.isdir(file_path):
            clear_directory(file_path)
            # After clearing the subdirectory, remove it
            os.rmdir(file_path)

def terminate_processes(processes):
    for process in processes:
        process.terminate()  # Politely ask the process to terminate
        try:
            process.wait(timeout=2)  # Wait a bit for the process to terminate
        except subprocess.TimeoutExpired:
            process.kill()  # Forcefully kill the process if it doesn't terminate

def main(num_mappers, num_reducers, num_centroids, num_iterations, sleep_time):
    
    mappers = run_mapper(num_mappers,sleep_time)
    reducers = run_reducer(num_reducers,sleep_time)
    master = run_master(num_mappers, num_reducers, num_centroids, num_iterations)

    # clear the directories
    
    
    
    # You might want to add some logic to wait for the processes to finish
    # or some other coordination mechanism, depending on your use case.

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: run.py <num_mappers> <num_reducers> <num_centroids> <num_iterations> <sleep_time>")
        sys.exit(1)

    clear_directory('Data/Mappers')
    clear_directory('Data/Reducers')
    clear_directory('Data/Dump')
    if (os.path.exists('Data/initial_centroids.txt')):
        os.remove('Data/initial_centroids.txt')
    if (os.path.exists('Data/centroids.txt')):
        os.remove('Data/centroids.txt')
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
