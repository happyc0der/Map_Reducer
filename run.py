"""Launcher: spawns the mappers, the reducers and the master, each in its own window.

Usage:
    python run.py <num_mappers> <num_reducers> <num_centroids> <num_iterations> <sleep_time>

WINDOWS ONLY. The processes are started with ``start cmd.exe /k``, which is a
cmd.exe builtin, so this script does nothing useful on macOS or Linux. See the
README for the equivalent manual commands on those platforms.

Set ``virtualenv_python`` below to the interpreter you want the three programs
to run under (an absolute path to a virtualenv's ``python.exe`` works too).

``sleep_time`` is passed to every mapper and reducer and is the number of
seconds each of them waits before doing any work, which is the window in which
you can force-stop one of them to exercise failure Scenario 2. Pass 0 for a
normal run.

Each spawned window is left open (``/k``) so its logs stay readable; close the
windows to shut the processes down.
"""

import os
import subprocess
import sys

virtualenv_python = r'python.exe'
master_script_path = r'./Master.py'
mapper_script_path = r'./Mapper.py'
reducer_script_path = r'./Reducer.py'


def run_process(script_path, *args):
    """Open a new cmd.exe window running ``script_path`` with ``args``."""
    cmd = ['start', 'cmd.exe', '/k', virtualenv_python, script_path] + list(map(str, args))
    print(cmd)
    return subprocess.Popen(cmd, shell=True)


def run_mapper(num_mappers, sleep_time):
    return [run_process(mapper_script_path, i, sleep_time) for i in range(num_mappers)]


def run_reducer(num_reducers, sleep_time):
    return [run_process(reducer_script_path, i, sleep_time) for i in range(num_reducers)]


def run_master(num_mappers, num_reducers, num_centroids, num_iterations):
    return run_process(master_script_path, num_mappers, num_reducers, num_centroids, num_iterations)


def clear_directory(directory):
    """Recursively empty ``directory``, creating it first if it does not exist.

    ``Data/Mappers``, ``Data/Reducers`` and ``Data/Dump`` hold generated output
    and are gitignored, so they are missing in a fresh clone.
    """
    os.makedirs(directory, exist_ok=True)
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            clear_directory(file_path)
            os.rmdir(file_path)


def main(num_mappers, num_reducers, num_centroids, num_iterations, sleep_time):
    """Start the mappers and reducers first, then the master that drives them."""
    run_mapper(num_mappers, sleep_time)
    run_reducer(num_reducers, sleep_time)
    run_master(num_mappers, num_reducers, num_centroids, num_iterations)


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: run.py <num_mappers> <num_reducers> <num_centroids> <num_iterations> <sleep_time>")
        sys.exit(1)

    if os.name != "nt":
        print("run.py uses the cmd.exe 'start' builtin and only works on Windows.")
        print("On macOS/Linux start the processes by hand - see the README.")
        sys.exit(1)

    # Start from a clean slate so output from a previous run is never mistaken
    # for output of this one.
    clear_directory('Data/Mappers')
    clear_directory('Data/Reducers')
    clear_directory('Data/Dump')
    if os.path.exists('Data/initial_centroids.txt'):
        os.remove('Data/initial_centroids.txt')
    if os.path.exists('Data/centroids.txt'):
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
