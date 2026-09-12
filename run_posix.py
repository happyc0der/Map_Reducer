"""Launcher for macOS and Linux: the counterpart of run.py, which is Windows only.

Usage:
    python run_posix.py <num_mappers> <num_reducers> <num_centroids> <num_iterations> <sleep_time>

Same arguments as run.py. Instead of opening a terminal window per process
(which needs the cmd.exe `start` builtin) the mappers and reducers are started
as plain child processes with their output redirected to ``logs/``, and the
master runs in the foreground so you see its output directly. The workers are
shut down when the master exits, including on Ctrl-C.

This file also works on Windows -- you just get log files instead of windows.

``Cluster`` is importable, and test_kmeans.py uses it to bring a cluster up and
down around each test:

    with Cluster(num_mappers=3, num_reducers=2) as cluster:
        run_master(3, 2, 2, 10)
"""

import os
import socket
import subprocess
import sys
import time

MASTER_PORT = 4040
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(REPO_ROOT, "logs")

# Directories holding generated output, cleared at the start of a run.
GENERATED_DIRECTORIES = ["Data/Mappers", "Data/Reducers", "Data/Dump"]
GENERATED_FILES = ["Data/initial_centroids.txt", "Data/centroids.txt"]

# How long to wait for a worker's port to start accepting connections.
STARTUP_TIMEOUT_SECONDS = 15.0
# How long to wait for a worker to exit after being asked to terminate.
SHUTDOWN_TIMEOUT_SECONDS = 5.0


def mapper_port(index):
    """Port of mapper ``index`` (0-based on the command line, so id index + 1)."""
    return MASTER_PORT + index + 1


def reducer_port(index):
    """Port of reducer ``index`` (0-based on the command line, so id index + 1)."""
    return MASTER_PORT - index - 1


def port_is_open(port, host="localhost", timeout=0.2):
    """Return True when something is already listening on ``port``.

    Every address family getaddrinfo offers is tried: the workers bind the IPv6
    wildcard ("[::]"), and an IPv4-only probe can miss that on a platform whose
    IPv6 sockets do not accept IPv4 connections.
    """
    try:
        candidates = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return False
    for family, socket_type, protocol, _, address in candidates:
        with socket.socket(family, socket_type, protocol) as sock:
            sock.settimeout(timeout)
            if sock.connect_ex(address) == 0:
                return True
    return False


def reset_data_directories():
    """Empty the generated directories and delete the generated centroid files.

    Mirrors what run.py does on Windows, so output from a previous run can never
    be mistaken for output of this one.
    """
    for directory in GENERATED_DIRECTORIES:
        path = os.path.join(REPO_ROOT, directory)
        os.makedirs(path, exist_ok=True)
        _clear_directory(path)
    for file_name in GENERATED_FILES:
        path = os.path.join(REPO_ROOT, file_name)
        if os.path.exists(path):
            os.remove(path)


def _clear_directory(directory):
    for entry in os.listdir(directory):
        path = os.path.join(directory, entry)
        if os.path.isdir(path):
            _clear_directory(path)
            os.rmdir(path)
        else:
            os.remove(path)


class Cluster:
    """The mapper and reducer processes, started and stopped as a group.

    The workers are long-lived servers: they stay up across every iteration of a
    run and across several runs, so a test can start one cluster and drive
    several masters through it.
    """

    def __init__(self, num_mappers, num_reducers, sleep_time=0, python=None, log_dir=LOG_DIR):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.sleep_time = sleep_time
        self.python = python or sys.executable
        self.log_dir = log_dir
        self.mappers = []
        self.reducers = []
        self._log_files = []

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.stop()
        return False

    def start(self):
        """Spawn every worker and block until all of their ports are accepting."""
        busy = [
            port
            for port in [mapper_port(i) for i in range(self.num_mappers)]
            + [reducer_port(i) for i in range(self.num_reducers)]
            if port_is_open(port)
        ]
        if busy:
            raise RuntimeError(
                f"ports already in use: {busy}. A worker from an earlier run is still "
                "alive and would silently receive this run's traffic. Stop it first "
                "(e.g. pkill -f Mapper.py; pkill -f Reducer.py)."
            )

        os.makedirs(self.log_dir, exist_ok=True)
        for i in range(self.num_mappers):
            self.mappers.append(self._spawn("Mapper.py", i, f"mapper_{i + 1}.log"))
        for i in range(self.num_reducers):
            self.reducers.append(self._spawn("Reducer.py", i, f"reducer_{i + 1}.log"))

        self._wait_until_listening()
        return self

    def _spawn(self, script, index, log_name):
        log_path = os.path.join(self.log_dir, log_name)
        # Deliberately not a context manager: the handle has to stay open for as
        # long as the child writes to it. stop() closes it.
        log_file = open(log_path, "w")  # noqa: SIM115
        self._log_files.append(log_file)
        return subprocess.Popen(
            [self.python, script, str(index), str(self.sleep_time)],
            cwd=REPO_ROOT,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

    def _wait_until_listening(self):
        expected = [mapper_port(i) for i in range(self.num_mappers)]
        expected += [reducer_port(i) for i in range(self.num_reducers)]
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        pending = list(expected)
        while pending and time.monotonic() < deadline:
            pending = [port for port in pending if not port_is_open(port)]
            if pending:
                time.sleep(0.1)
        if pending:
            self.stop()
            raise RuntimeError(
                f"workers on ports {pending} did not come up within "
                f"{STARTUP_TIMEOUT_SECONDS:g}s. Check the logs in {self.log_dir}."
            )

    def stop(self):
        """Terminate every worker, escalating to a kill if one ignores that."""
        for process in self.mappers + self.reducers:
            if process.poll() is None:
                process.terminate()
        for process in self.mappers + self.reducers:
            try:
                process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
        self.mappers = []
        self.reducers = []
        for log_file in self._log_files:
            log_file.close()
        self._log_files = []


def run_master(num_mappers, num_reducers, num_centroids, num_iterations, python=None, capture=False):
    """Run Master.py to completion in the foreground.

    Returns the CompletedProcess. With ``capture`` set, stdout and stderr are
    captured instead of inherited, which is what the tests want.
    """
    command = [
        python or sys.executable,
        "Master.py",
        str(num_mappers),
        str(num_reducers),
        str(num_centroids),
        str(num_iterations),
    ]
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=capture,
        text=True,
        # The three programs reconfigure their own stdio to UTF-8 so their emoji
        # survive being redirected, so decode as UTF-8 rather than the locale
        # encoding.
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def main(argv):
    if len(argv) != 6:
        print("Usage: run_posix.py <num_mappers> <num_reducers> <num_centroids> <num_iterations> <sleep_time>")
        return 1

    num_mappers, num_reducers, num_centroids, num_iterations, sleep_time = (int(value) for value in argv[1:])
    print("Number of Mappers: ", num_mappers, flush=True)
    print("Number of Reducers: ", num_reducers, flush=True)
    print("Number of Centroids: ", num_centroids, flush=True)
    print("Number of Iterations: ", num_iterations, flush=True)
    print("Sleep Time: ", sleep_time, flush=True)

    reset_data_directories()
    with Cluster(num_mappers, num_reducers, sleep_time) as cluster:
        print(
            f"{len(cluster.mappers)} mapper(s) and {len(cluster.reducers)} reducer(s) up; "
            f"logs in {LOG_DIR}",
            flush=True,
        )
        result = run_master(num_mappers, num_reducers, num_centroids, num_iterations)
    return result.returncode


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        # The Cluster context manager has already shut the workers down.
        print("\ninterrupted")
        sys.exit(130)
