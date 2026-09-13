"""Tests for the MapReduce K-Means pipeline, organised around the assignment rubric.

Run everything -- nothing may be listening on the mapper/reducer ports
beforehand, because each test brings up its own cluster:

    python -m unittest -v test_kmeans

Run one class, or one test:

    python -m unittest -v test_kmeans.TestOutput_3M_2R_3K
    python -m unittest -v test_kmeans.TestEndToEnd.test_matches_reference_kmeans

The fault-injection tests are slow, because they have to wait inside a worker's
sleep window to kill it, so they are skipped unless you ask for them:

    RUN_FAULT_TESTS=1 python -m unittest -v test_kmeans

Those tests kill a worker half way through its sleep window; on a slow machine
widen the window with FAULT_SLEEP_SECONDS=15.

Only the standard library is needed, plus the grpcio and protobuf the pipeline
already needs.

Coverage of the assignment
--------------------------
Evaluation criteria:

1. "test the master with different numbers of mappers, reducers, centroids and
   iterations" -- OUTPUT_CONFIGURATIONS below drives the whole output-verification
   suite over four shardings (1x1, 3x2, 4x3, 8x2) with K of 2 and 3 and both a
   single iteration and a converging run. TestEndToEnd adds the same spread for
   the end-to-end result.
3. "the outputs of a. Mappers b. Reducers will be verified for different
   configurations" -- every test in OutputVerification runs once per
   configuration: partition layout, partition routing, split coverage, map keys,
   reducer line format, reducer means, and centroid compilation.

Implementation details:

* Master parameters (M, R, K, iterations) -- TestEndToEnd.
* Input split, mapper reads its own chunk, master ships no data --
  OutputVerification.test_each_mapper_only_writes_its_own_split and
  TestProtocol.test_the_map_request_carries_indices_not_data_points.
* Map output key/value, written to the mapper's own directory --
  OutputVerification.test_every_point_is_keyed_to_its_nearest_centroid,
  test_the_directory_layout_matches_the_specification.
* Partition: R partitions per mapper, M*R in total, same key to the same
  partition -- OutputVerification.test_there_are_m_times_r_partitions,
  test_a_key_only_ever_lands_in_one_partition.
* Shuffle and sort, and "reducers must not read the intermediate files, they
  must make gRPC calls to the mappers" --
  TestProtocol.test_every_reducer_pulls_from_every_mapper_over_grpc.
* Reduce output is the updated centroid, in the reducer's own directory --
  OutputVerification.test_a_reduced_centroid_is_the_mean_of_its_group,
  test_reducer_output_has_well_formed_lines_owned_by_that_reducer.
* Centroid compilation into a single file, random initial centroids from the
  input -- OutputVerification.test_final_centroids_are_compiled_from_the_reducer_output,
  TestEndToEnd.test_the_initial_centroids_are_sampled_from_the_input.
* Fault tolerance, both scenarios -- TestFaultTolerance (Scenario 2, force-stop)
  and TestScenarioOneFailures (the injected self-reported failure).
* Required print/dump output: iteration number, the gRPC calls, SUCCESS/FAILURE
  per worker, and the centroids of each iteration including the initial ones --
  TestLogging.

The central correctness test is TestEndToEnd.test_matches_reference_kmeans: it
runs the distributed pipeline, then replays the same K-Means from the same
randomly sampled starting centroids with a plain single-process implementation
(``reference_kmeans``, written from scratch as the assignment requires), and
asserts the two agree. That turns "it produced some centroids" into "it produced
the right centroids".

``reference_kmeans`` mirrors Master.py's loop exactly, including two details that
matter for a tight comparison:

* ties go to the lowest centroid index, because Mapper.calculate_min_distance
  compares with a strict ``<``;
* when the loop stops on convergence the master writes the centroids from
  *before* that iteration's update, so the reference does too. The two differ by
  less than EPSILON either way, but replicating it keeps the tolerance small
  enough to catch a real error.
"""

import ast
import contextlib
import importlib.util
import io
import os
import re
import subprocess
import sys
import time
import unittest
from unittest import mock

import mapreduce_pb2
from run_posix import (
    REPO_ROOT,
    Cluster,
    mapper_port,
    port_is_open,
    reducer_port,
    reset_data_directories,
    run_master,
)

INPUT_PATH = os.path.join(REPO_ROOT, "Data/Input/points.txt")
INITIAL_CENTROIDS_PATH = os.path.join(REPO_ROOT, "Data/initial_centroids.txt")
FINAL_CENTROIDS_PATH = os.path.join(REPO_ROOT, "Data/centroids.txt")
MAPPERS_DIR = os.path.join(REPO_ROOT, "Data/Mappers")
REDUCERS_DIR = os.path.join(REPO_ROOT, "Data/Reducers")
DUMP_DIR = os.path.join(REPO_ROOT, "Data/Dump")
MASTER_DUMP_PATH = os.path.join(DUMP_DIR, "master_dump.txt")

# Must match Master.EPSILON.
EPSILON = 0.0001
# Slack allowed between the pipeline and the reference: the two sum the same
# values in a different order, so they agree to about machine precision.
TOLERANCE = 1e-6

# (mappers, reducers, centroids, iterations) the output suite is run over, to
# satisfy "the outputs of mappers and reducers will be verified for different
# configurations".
OUTPUT_CONFIGURATIONS = [
    (1, 1, 2, 5),   # degenerate: nothing is actually shared out
    (3, 2, 3, 5),   # more mappers than reducers, K not a multiple of R
    (4, 3, 3, 8),   # K == R, so every reducer owns exactly one centroid
    (8, 2, 2, 1),   # many small splits, and a single iteration
]

EMPTY_CLUSTER_EXPLANATION = (
    "an iteration left a centroid with no points assigned to it. The master "
    "indexes the compiled centroid list positionally, and a centroid that "
    "attracted no points is absent from it, so the run cannot complete. This "
    "depends on the randomly sampled starting centroids, not on the code under "
    "test -- see the known limitations in the README."
)


def read_points(path=INPUT_PATH):
    """Read an "x,y" per line file into a list of [x, y] pairs."""
    points = []
    with open(path) as file:
        for line in file:
            line = line.strip()
            if line:
                x, y = line.split(",")
                points.append([float(x), float(y)])
    return points


class EmptyClusterError(Exception):
    """An iteration left a centroid with no points assigned to it."""


def assign_to_nearest(point, centroids):
    """Return the index of the centroid nearest to ``point`` (ties to the lowest index)."""
    best_index = -1
    best_distance = float("inf")
    for index, centroid in enumerate(centroids):
        distance = (point[0] - centroid[0]) ** 2 + (point[1] - centroid[1]) ** 2
        if distance < best_distance:
            best_distance = distance
            best_index = index
    return best_index


def group_by_nearest(points, centroids):
    """Group ``points`` by the index of their nearest centroid."""
    groups = {index: [] for index in range(len(centroids))}
    for point in points:
        groups[assign_to_nearest(point, centroids)].append(point)
    return groups


def reference_kmeans(points, initial_centroids, num_iterations):
    """Single-process K-Means mirroring Master.py's loop.

    Returns ``(centroids, iterations_run, converged)``.
    """
    centroids = [list(centroid) for centroid in initial_centroids]
    for iteration in range(num_iterations):
        groups = group_by_nearest(points, centroids)
        new_centroids = []
        for key in sorted(groups):
            group = groups[key]
            if not group:
                raise EmptyClusterError(
                    f"centroid {key} attracted no points in iteration {iteration + 1}"
                )
            count = len(group)
            new_centroids.append([
                sum(point[0] for point in group) / count,
                sum(point[1] for point in group) / count,
            ])

        converged = all(
            abs(old[0] - new[0]) < EPSILON and abs(old[1] - new[1]) < EPSILON
            for old, new in zip(centroids, new_centroids, strict=True)
        )
        if converged:
            # Master.py breaks before adopting new_centroids.
            return centroids, iteration + 1, True
        centroids = new_centroids
    return centroids, num_iterations, False


def run_pipeline(num_mappers, num_reducers, num_centroids, num_iterations, sleep_time=0):
    """Bring a cluster up, drive one master through it, and tear the cluster down.

    Raises ``unittest.SkipTest`` when the run hit the empty-cluster limitation,
    and ``AssertionError`` when the master failed for any other reason.
    """
    reset_data_directories()
    with Cluster(num_mappers, num_reducers, sleep_time):
        result = run_master(num_mappers, num_reducers, num_centroids, num_iterations, capture=True)
    if result.returncode != 0:
        if "check_convergence" in result.stderr and "IndexError" in result.stderr:
            raise unittest.SkipTest(EMPTY_CLUSTER_EXPLANATION)
        raise AssertionError(
            f"master exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def _repeated_field_names(message_class):
    """Names of the repeated fields of a protobuf message.

    ``FieldDescriptor.is_repeated`` was added in protobuf 5.27 and ``label`` was
    removed in 7.x, so both spellings are supported.
    """
    names = []
    for field in message_class.DESCRIPTOR.fields:
        if hasattr(field, "is_repeated"):
            repeated = field.is_repeated
        else:  # protobuf < 5.27
            repeated = field.label == field.LABEL_REPEATED
        if repeated:
            names.append(field.name)
    return names


def load_master_module():
    """Import Master.py as a module, without running its __main__ block.

    Lets a test call the master's functions directly, for paths that cannot be
    reached by driving the pipeline from outside.
    """
    spec = importlib.util.spec_from_file_location(
        "master_under_test", os.path.join(REPO_ROOT, "Master.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_master_dump():
    with open(MASTER_DUMP_PATH) as file:
        return file.read()


def read_worker_dump(name):
    with open(os.path.join(DUMP_DIR, name)) as file:
        return file.read()


def read_iteration_input_centroids():
    """Centroid lists the master handed to the mappers, one entry per iteration.

    Parsed out of the master's dump, which logs them before each iteration.
    """
    return [
        ast.literal_eval(match)
        for match in re.findall(r"Starting Iteration \d+ wtih centroids (\[.*\])", read_master_dump())
    ]


def read_partition_lines(num_mappers, num_reducers):
    """Yield ``(path, line_number, key, point)`` for every mapped pair on disk."""
    for i in range(num_mappers):
        for p in range(num_reducers):
            path = os.path.join(MAPPERS_DIR, f"M{i + 1}", f"partition_{p + 1}.txt")
            with open(path) as file:
                for line_number, line in enumerate(file, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    key, x, y = line.split(",")
                    yield path, line_number, int(key), [float(x), float(y)]


class CentroidAssertions(unittest.TestCase):
    """Shared comparison helpers."""

    def assertCentroidsAlmostEqual(self, actual, expected, tolerance=TOLERANCE):
        self.assertEqual(
            len(actual), len(expected),
            f"expected {len(expected)} centroids, got {len(actual)}:\n"
            f"  actual:   {actual}\n  expected: {expected}",
        )
        for index, (got, want) in enumerate(zip(actual, expected, strict=True)):
            self.assertAlmostEqual(
                got[0], want[0], delta=tolerance,
                msg=f"centroid {index} x: pipeline {got[0]!r} vs reference {want[0]!r}",
            )
            self.assertAlmostEqual(
                got[1], want[1], delta=tolerance,
                msg=f"centroid {index} y: pipeline {got[1]!r} vs reference {want[1]!r}",
            )

    def assertMatchesReference(self, num_iterations):
        """Compare Data/centroids.txt with a reference run from the same start."""
        try:
            expected, _, _ = reference_kmeans(
                read_points(), read_points(INITIAL_CENTROIDS_PATH), num_iterations
            )
        except EmptyClusterError as error:
            self.skipTest(f"{error}: {EMPTY_CLUSTER_EXPLANATION}")
        self.assertCentroidsAlmostEqual(read_points(FINAL_CENTROIDS_PATH), expected)


class TestReferenceImplementation(CentroidAssertions):
    """The reference K-Means itself, so a broken reference cannot hide a bug."""

    def test_one_step_on_a_hand_worked_example(self):
        points = [[0.0, 0.0], [2.0, 0.0], [10.0, 0.0], [12.0, 0.0]]
        centroids, iterations, converged = reference_kmeans(points, [[0.0, 0.0], [10.0, 0.0]], 1)
        self.assertEqual(iterations, 1)
        self.assertFalse(converged)
        self.assertCentroidsAlmostEqual(centroids, [[1.0, 0.0], [11.0, 0.0]], tolerance=0.0)

    def test_it_converges_and_stops_early(self):
        points = [[0.0, 0.0], [2.0, 0.0], [10.0, 0.0], [12.0, 0.0]]
        centroids, iterations, converged = reference_kmeans(points, [[0.0, 0.0], [10.0, 0.0]], 50)
        self.assertTrue(converged)
        self.assertLess(iterations, 50)
        self.assertCentroidsAlmostEqual(centroids, [[1.0, 0.0], [11.0, 0.0]], tolerance=0.0)

    def test_ties_go_to_the_lowest_centroid_index(self):
        self.assertEqual(assign_to_nearest([1.0, 0.0], [[0.0, 0.0], [2.0, 0.0]]), 0)

    def test_an_empty_cluster_is_reported(self):
        with self.assertRaises(EmptyClusterError):
            reference_kmeans([[0.0, 0.0], [1.0, 0.0]], [[0.0, 0.0], [100.0, 100.0]], 1)


class TestProtocol(CentroidAssertions):
    """The gRPC contract the assignment prescribes."""

    def test_the_map_request_carries_indices_not_data_points(self):
        """"Master should not send the input data points to the mapper".

        The split is described by an index range, so the only repeated field in
        MapRequest is the centroid list. This fails if a field that could carry
        the data itself is ever added.
        """
        fields = set(mapreduce_pb2.MapRequest.DESCRIPTOR.fields_by_name)
        self.assertEqual(fields, {"begin", "end", "centroids", "num_reducers", "append"})
        self.assertEqual(_repeated_field_names(mapreduce_pb2.MapRequest), ["centroids"])

    def test_the_service_defines_the_four_prescribed_calls(self):
        service = mapreduce_pb2.DESCRIPTOR.services_by_name["MapReduceService"]
        self.assertEqual(
            sorted(method.name for method in service.methods),
            ["Map", "Reduce", "StartReduce", "returnCentroid"],
        )

    def test_every_reducer_pulls_from_every_mapper_over_grpc(self):
        """"Reducers should not read their input from the intermediate files".

        Each reducer must call Reduce on every mapper once per iteration, so each
        mapper logs exactly ``iterations * R`` incoming Reduce requests.
        """
        num_mappers, num_reducers = 3, 2
        run_pipeline(num_mappers, num_reducers, num_centroids=2, num_iterations=3)
        iterations = len(read_iteration_input_centroids())
        if "Scenario 2" in read_master_dump():
            self.skipTest("a worker was redirected, so the call counts are not exact")

        for i in range(num_mappers):
            dump = read_worker_dump(f"M{i + 1}_dump.txt")
            self.assertEqual(
                dump.count("Received a Reduce Request from Reducer."),
                iterations * num_reducers,
                f"mapper {i + 1} was not asked for its partitions by every reducer "
                f"in every one of the {iterations} iteration(s)",
            )
        for i in range(num_reducers):
            dump = read_worker_dump(f"R{i + 1}_dump.txt")
            self.assertEqual(dump.count("Sending Reduce Request to all Mappers"), iterations)
            for mapper_id in range(1, num_mappers + 1):
                self.assertIn(f"Received a Reduce Response from Mapper id {mapper_id}", dump)


class TestRequestConstructionFailure(CentroidAssertions):
    """The master's guards around building a request message.

    Building a request is local work, but it allocates, so it can fail under
    memory pressure -- and MemoryError is an Exception, so these guards catch it.
    Each one has to report the failure and return, because the alternative is
    that a failure local to the master gets reported as a dead worker and its
    task handed to a different one.

    Neither path can be reached by driving the pipeline from outside, so the
    allocation failure is injected directly.
    """

    def _inject_failure(self, message_name, call):
        """Make one message class raise, then run ``call`` and capture the console."""
        reset_data_directories()
        master = load_master_module()
        console = io.StringIO()
        with mock.patch.object(
            master.mapreduce_pb2,
            message_name,
            side_effect=MemoryError("simulated allocation failure"),
        ), contextlib.redirect_stdout(console):
            # Must not raise, and must not return a redirected retry.
            self.assertIsNone(call(master))
        return console.getvalue(), read_master_dump()

    def test_a_start_reduce_request_failure_does_not_look_like_a_dead_reducer(self):
        console, dump = self._inject_failure(
            "StartReduceRequest",
            lambda master: master.compose_reduce_request(4039, 1, 1, 1),
        )
        self.assertIn("❌ Error in Start Reduce Request", console)
        self.assertIn("Could not build the Start Reduce Request for Reducer with id 1", dump)
        self.assertNotIn("redirecting to next reducer", dump)
        self.assertNotIn("Scenario 2", dump)

    def test_a_map_request_failure_does_not_look_like_a_dead_mapper(self):
        console, dump = self._inject_failure(
            "MapRequest",
            lambda master: master.compose_map_request(0, 5, [[0.0, 0.0]], 4041, 1, 1),
        )
        self.assertIn("❌ Error in Map Request", console)
        self.assertNotIn("retrying...", console)
        self.assertIn("Could not build the Map Request for Mapper with id 1", dump)
        self.assertNotIn("redirecting to next mapper", dump)
        self.assertNotIn("Scenario 2", dump)


class TestEndToEnd(CentroidAssertions):
    """The distributed result against a plain single-process K-Means."""

    def test_matches_reference_kmeans(self):
        run_pipeline(num_mappers=3, num_reducers=2, num_centroids=2, num_iterations=10)
        self.assertMatchesReference(num_iterations=10)

    def test_matches_reference_kmeans_for_a_single_iteration(self):
        """One iteration of the pipeline is one averaging step of plain K-Means."""
        run_pipeline(num_mappers=2, num_reducers=2, num_centroids=2, num_iterations=1)
        self.assertMatchesReference(num_iterations=1)

    def test_matches_reference_kmeans_with_one_mapper_and_one_reducer(self):
        """The degenerate 1x1 sharding, where no partitioning work is shared out."""
        run_pipeline(num_mappers=1, num_reducers=1, num_centroids=3, num_iterations=10)
        self.assertMatchesReference(num_iterations=10)

    def test_matches_reference_kmeans_when_sharded_more_widely(self):
        """More mappers than reducers, and more of both: the answer must not change."""
        run_pipeline(num_mappers=4, num_reducers=3, num_centroids=3, num_iterations=10)
        self.assertMatchesReference(num_iterations=10)

    def test_matches_reference_kmeans_with_many_small_splits(self):
        """8 mappers over 25 points is 3 points each, and 4 for the last one."""
        run_pipeline(num_mappers=8, num_reducers=2, num_centroids=2, num_iterations=10)
        self.assertMatchesReference(num_iterations=10)

    def test_it_stops_at_the_iteration_limit_when_it_has_not_converged(self):
        """With a single iteration allowed, exactly one iteration must be logged."""
        run_pipeline(num_mappers=2, num_reducers=2, num_centroids=2, num_iterations=1)
        self.assertEqual(len(read_iteration_input_centroids()), 1)

    def test_it_stops_early_once_it_has_converged(self):
        """A generous iteration budget must not be used up once the centroids settle."""
        result = run_pipeline(num_mappers=3, num_reducers=2, num_centroids=2, num_iterations=50)
        if "Converged in Iteration" not in read_master_dump():
            self.skipTest("this run did not converge within 50 iterations")
        self.assertLess(len(read_iteration_input_centroids()), 50)
        self.assertIn("🏁 Converged in", result.stdout)

    def test_reducers_are_clamped_to_the_centroid_count(self):
        """Asking for more reducers than centroids still produces K centroids."""
        run_pipeline(num_mappers=2, num_reducers=4, num_centroids=2, num_iterations=5)
        self.assertEqual(len(read_points(FINAL_CENTROIDS_PATH)), 2)
        # Reducers 3 and 4 are clamped away and never write an output file.
        self.assertEqual(sorted(os.listdir(REDUCERS_DIR)), ["R1.txt", "R2.txt"])

    def test_the_initial_centroids_are_sampled_from_the_input(self):
        """"Before the first iteration, the centroids should be randomly selected
        from the input data points"."""
        run_pipeline(num_mappers=2, num_reducers=2, num_centroids=3, num_iterations=2)
        initial = read_points(INITIAL_CENTROIDS_PATH)
        points = [tuple(point) for point in read_points()]
        self.assertEqual(len(initial), 3)
        self.assertEqual(len({tuple(point) for point in initial}), 3, "sampled with repetition")
        for centroid in initial:
            self.assertIn(tuple(centroid), points)


class OutputVerification:
    """Mapper and reducer output checks, run once per configuration.

    Not a TestCase itself, so unittest does not collect it directly -- the
    concrete TestOutput_* classes below mix it with CentroidAssertions. One
    pipeline run is shared by the whole class; every test here only reads the
    files it left behind.
    """

    NUM_MAPPERS = 0
    NUM_REDUCERS = 0
    NUM_CENTROIDS = 0
    NUM_ITERATIONS = 0

    @classmethod
    def setUpClass(cls):
        cls.result = run_pipeline(
            cls.NUM_MAPPERS, cls.NUM_REDUCERS, cls.NUM_CENTROIDS, cls.NUM_ITERATIONS
        )

    def test_the_directory_layout_matches_the_specification(self):
        """The tree the assignment's "Sample input and output files" section gives."""
        self.assertTrue(os.path.isfile(INPUT_PATH))
        self.assertTrue(os.path.isfile(INITIAL_CENTROIDS_PATH))
        self.assertTrue(os.path.isfile(FINAL_CENTROIDS_PATH))
        self.assertEqual(
            sorted(os.listdir(MAPPERS_DIR)),
            [f"M{i + 1}" for i in range(self.NUM_MAPPERS)],
        )
        self.assertEqual(
            sorted(os.listdir(REDUCERS_DIR)),
            [f"R{i + 1}.txt" for i in range(self.NUM_REDUCERS)],
        )

    def test_there_are_m_times_r_partitions(self):
        """"If there are M mappers and R reducers, each mapper should have R file
        partitions ... M*R partitions in total"."""
        total = 0
        for i in range(self.NUM_MAPPERS):
            partitions = sorted(os.listdir(os.path.join(MAPPERS_DIR, f"M{i + 1}")))
            self.assertEqual(
                partitions,
                [f"partition_{p + 1}.txt" for p in range(self.NUM_REDUCERS)],
            )
            total += len(partitions)
        self.assertEqual(total, self.NUM_MAPPERS * self.NUM_REDUCERS)

    def test_a_key_only_ever_lands_in_one_partition(self):
        """"The partitioning function should ensure that all key-value pairs with
        the same key are sent to the same partition"."""
        for path, line_number, key, _ in read_partition_lines(self.NUM_MAPPERS, self.NUM_REDUCERS):
            expected_partition = (key % self.NUM_REDUCERS) + 1
            actual_partition = int(re.search(r"partition_(\d+)", path).group(1))
            self.assertEqual(
                expected_partition, actual_partition,
                f"{path}:{line_number}: key {key} belongs in partition "
                f"{expected_partition}, not {actual_partition}",
            )

    def test_every_input_point_is_mapped_exactly_once(self):
        """The input split must lose no point and duplicate none."""
        mapped = sorted(
            tuple(point)
            for _, _, _, point in read_partition_lines(self.NUM_MAPPERS, self.NUM_REDUCERS)
        )
        self.assertEqual(mapped, sorted(tuple(point) for point in read_points()))

    def test_each_mapper_only_writes_its_own_split(self):
        """"Each mapper should process a different chunk of the file input data"."""
        points = read_points()
        divide = len(points) // self.NUM_MAPPERS
        for i in range(self.NUM_MAPPERS):
            begin = i * divide
            end = len(points) if i == self.NUM_MAPPERS - 1 else begin + divide
            expected = sorted(tuple(point) for point in points[begin:end])
            written = []
            for p in range(self.NUM_REDUCERS):
                path = os.path.join(MAPPERS_DIR, f"M{i + 1}", f"partition_{p + 1}.txt")
                with open(path) as file:
                    for line in file:
                        line = line.strip()
                        if line:
                            _, x, y = line.split(",")
                            written.append((float(x), float(y)))
            self.assertEqual(
                sorted(written), expected,
                f"mapper {i + 1} should have mapped indices [{begin}, {end})",
            )

    def test_every_point_is_keyed_to_its_nearest_centroid(self):
        """"Key: index of the nearest centroid ... Value: the data point itself".

        Compared against the centroids the master actually sent for the final
        iteration, which is what these files were written from.
        """
        centroids = read_iteration_input_centroids()[-1]
        for path, line_number, key, point in read_partition_lines(self.NUM_MAPPERS, self.NUM_REDUCERS):
            nearest = assign_to_nearest(point, centroids)
            self.assertEqual(
                key, nearest,
                f"{path}:{line_number}: point {point} is nearest to centroid "
                f"{nearest}, not {key}",
            )

    def test_reducer_output_has_well_formed_lines_owned_by_that_reducer(self):
        """R<i>.txt lines are "<centroid id> <x> <y>" for keys that reducer owns."""
        for i in range(self.NUM_REDUCERS):
            path = os.path.join(REDUCERS_DIR, f"R{i + 1}.txt")
            with open(path) as file:
                lines = [line.strip() for line in file if line.strip()]
            self.assertTrue(lines, f"{path} is empty")
            for line in lines:
                self.assertRegex(line, r"^\d+ -?[\d.]+(e-?\d+)? -?[\d.]+(e-?\d+)?$")
                key = int(line.split(" ")[0])
                self.assertEqual(
                    (key % self.NUM_REDUCERS) + 1, i + 1,
                    f"{path}: centroid {key} should have been reduced by reducer "
                    f"{(key % self.NUM_REDUCERS) + 1}",
                )

    def test_a_reduced_centroid_is_the_mean_of_its_group(self):
        """"Output of the reduce function: Key: Centroid Id, Value: Updated Centroid"."""
        centroids = read_iteration_input_centroids()[-1]
        groups = {}
        for _, _, key, point in read_partition_lines(self.NUM_MAPPERS, self.NUM_REDUCERS):
            groups.setdefault(key, []).append(point)

        for key, group in groups.items():
            count = len(group)
            expected = [
                sum(point[0] for point in group) / count,
                sum(point[1] for point in group) / count,
            ]
            owner = (key % self.NUM_REDUCERS) + 1
            with open(os.path.join(REDUCERS_DIR, f"R{owner}.txt")) as file:
                reduced = [line.split(" ") for line in file if line.strip()]
            newest = [
                [float(fields[1]), float(fields[2])] for fields in reduced if int(fields[0]) == key
            ][-1]
            self.assertCentroidsAlmostEqual([newest], [expected])
        # Every centroid must have attracted points, or the run could not have
        # finished at all.
        self.assertEqual(sorted(groups), list(range(len(centroids))))

    def test_final_centroids_are_compiled_from_the_reducer_output(self):
        """"The master needs to parse the output generated by all the reducers to
        compile the final list of (K) centroids and store them in a single file".

        Which block of reducer output it equals depends on how the run ended. The
        master compiles the newest value per key every iteration, but on the
        converging iteration it breaks *before* adopting them, so what it writes
        is the list it started that iteration with. Those two differ by less than
        EPSILON, and this pins down both cases rather than papering over the
        difference with a loose tolerance.
        """
        newest = {}
        for i in range(self.NUM_REDUCERS):
            with open(os.path.join(REDUCERS_DIR, f"R{i + 1}.txt")) as file:
                for line in file:
                    line = line.strip()
                    if line:
                        key, x, y = line.split(" ")
                        newest[int(key)] = [float(x), float(y)]
        compiled = [newest[key] for key in sorted(newest)]
        final = read_points(FINAL_CENTROIDS_PATH)
        self.assertEqual(len(final), self.NUM_CENTROIDS)

        if "Converged in Iteration" in read_master_dump():
            self.assertCentroidsAlmostEqual(
                final, read_iteration_input_centroids()[-1], tolerance=0.0
            )
            for index, (got, want) in enumerate(zip(final, compiled, strict=True)):
                self.assertLess(abs(got[0] - want[0]), EPSILON, f"centroid {index} x")
                self.assertLess(abs(got[1] - want[1]), EPSILON, f"centroid {index} y")
        else:
            self.assertCentroidsAlmostEqual(final, compiled, tolerance=0.0)


def _make_output_test_classes():
    """Create one OutputVerification subclass per entry in OUTPUT_CONFIGURATIONS."""
    for num_mappers, num_reducers, num_centroids, num_iterations in OUTPUT_CONFIGURATIONS:
        name = f"TestOutput_{num_mappers}M_{num_reducers}R_{num_centroids}K"
        globals()[name] = type(name, (OutputVerification, CentroidAssertions), {
            "__doc__": (
                f"Mapper and reducer output for M={num_mappers}, R={num_reducers}, "
                f"K={num_centroids}, {num_iterations} iteration(s)."
            ),
            "NUM_MAPPERS": num_mappers,
            "NUM_REDUCERS": num_reducers,
            "NUM_CENTROIDS": num_centroids,
            "NUM_ITERATIONS": num_iterations,
        })


_make_output_test_classes()


class TestLogging(CentroidAssertions):
    """The print/dump output the assignment asks for."""

    NUM_ITERATIONS = 3

    @classmethod
    def setUpClass(cls):
        cls.result = run_pipeline(2, 2, 2, cls.NUM_ITERATIONS)

    def test_every_process_writes_a_dump_file(self):
        """"Please log/print everything in a dump.txt file"."""
        self.assertEqual(
            sorted(os.listdir(DUMP_DIR)),
            ["M1_dump.txt", "M2_dump.txt", "R1_dump.txt", "R2_dump.txt", "master_dump.txt"],
        )

    def test_the_master_dump_numbers_its_iterations_consecutively(self):
        """Required output 1: the iteration number."""
        iterations = re.findall(r"Starting Iteration (\d+)", read_master_dump())
        self.assertTrue(iterations, "the dump records no iteration")
        self.assertEqual(iterations, [str(i + 1) for i in range(len(iterations))])
        self.assertLessEqual(len(iterations), self.NUM_ITERATIONS)

    def test_the_master_dump_records_every_phase(self):
        """Required output 2: the execution of the gRPC calls to mappers and reducers."""
        dump = read_master_dump()
        for phase in [
            "Input Split by Master Done",
            "Sending Map Request to Mappers",
            "Mapping Done",
            "Sending Reduce Request to all Reducers",
            "Asking Reducers to send the centroid data to Master",
            "Received all the centroid data from Reducers",
            "Centroid Compilation is done",
            "Finished writing the final centroids to the file",
        ]:
            self.assertIn(phase, dump)

    def test_a_success_or_failure_is_recorded_for_every_worker(self):
        """Required output 3: the gRPC response for each mapper and reducer function."""
        for mapper_id in (1, 2):
            self.assertIn(
                f"Recieved a Map Response from ID {mapper_id}: status OK", self.result.stdout
            )
        for reducer_id in (1, 2):
            self.assertIn(
                f"Received an ack for starting reduciton from Reducer with id {reducer_id} : SUCCESS",
                read_master_dump(),
            )
            self.assertIn(
                f"Recieved a Return Reduce Response from PORT {reducer_port(reducer_id - 1)}: status 1",
                self.result.stdout,
            )

    def test_the_centroids_of_every_iteration_are_reported(self):
        """Required output 4: the centroids after each iteration, and the initial ones."""
        logged = read_iteration_input_centroids()
        self.assertEqual(len(logged), self.result.stdout.count("old centroid"))
        self.assertIn("new centroid", self.result.stdout)
        # The first iteration is logged with the randomly initialised centroids.
        self.assertCentroidsAlmostEqual(
            logged[0], read_points(INITIAL_CENTROIDS_PATH), tolerance=0.0
        )

    def test_the_dump_and_the_console_agree_on_convergence(self):
        self.assertEqual(
            "Converged in Iteration" in read_master_dump(),
            "🏁 Converged in" in self.result.stdout,
        )

    def test_the_workers_log_their_own_side_of_the_conversation(self):
        mapper_dump = read_worker_dump("M1_dump.txt")
        self.assertIn("Hello, I am a Mapper 1", mapper_dump)
        self.assertIn("Received a Map Request from Master.", mapper_dump)
        self.assertIn("Received a Reduce Request from Reducer.", mapper_dump)

        reducer_dump = read_worker_dump("R1_dump.txt")
        self.assertIn("Recieved a Start Reduce Request from Master", reducer_dump)
        self.assertIn("Shuffling and sorting the data received from Mappers", reducer_dump)
        self.assertIn("Recieved a Request from Master to return the centroids.", reducer_dump)


class TestScenarioOneFailures(CentroidAssertions):
    """Failure Scenario 1: the worker reports the task as failed but stays alive.

    Mapper.py and Reducer.py inject this with probability
    1 - FAILURE_FREE_PROBABILITY, so it cannot be forced. Runs are repeated until
    one is observed, and the test then checks the master retried and still got
    the right answer.
    """

    MAX_ATTEMPTS = 12

    def test_an_injected_failure_is_retried_and_the_result_is_still_correct(self):
        for _ in range(self.MAX_ATTEMPTS):
            run_pipeline(num_mappers=3, num_reducers=2, num_centroids=2, num_iterations=10)
            dump = read_master_dump()
            if "Failed like Scenario 1, retrying..." in dump:
                # Whichever side failed, the master retried the same worker and
                # the outcome must be indistinguishable from a clean run.
                self.assertMatchesReference(num_iterations=10)
                mapper_retries = len(re.findall(r"Mapper \d+ Failed like Scenario 1", dump))
                reducer_retries = len(re.findall(r"Reducer \d+ Failed like Scenario 1", dump))
                self.assertGreater(mapper_retries + reducer_retries, 0)
                return
        self.skipTest(
            f"no Scenario 1 failure was injected in {self.MAX_ATTEMPTS} runs "
            "(each one is a 5% chance per call)"
        )


@unittest.skipUnless(
    os.environ.get("RUN_FAULT_TESTS"),
    "slow: set RUN_FAULT_TESTS=1 to run the fault-injection tests",
)
class TestFaultTolerance(CentroidAssertions):
    """Failure Scenario 2: "we may force stop a mapper or reducer process"."""

    # The window in which the victim is killed. A slow or loaded machine needs a
    # wider one, so CI raises it via FAULT_SLEEP_SECONDS.
    SLEEP_TIME = int(os.environ.get("FAULT_SLEEP_SECONDS", "6"))
    NUM_ITERATIONS = 3

    def _run_with_one_worker_killed(self, kind, index, num_mappers, num_reducers, num_centroids):
        """Start a run, kill one worker inside its sleep window, return the master's result."""
        reset_data_directories()
        with Cluster(num_mappers, num_reducers, sleep_time=self.SLEEP_TIME) as cluster:
            victim = (cluster.mappers if kind == "mapper" else cluster.reducers)[index]
            port = mapper_port(index) if kind == "mapper" else reducer_port(index)

            master = subprocess.Popen(
                [
                    sys.executable, "Master.py",
                    str(num_mappers), str(num_reducers), str(num_centroids), str(self.NUM_ITERATIONS),
                ],
                cwd=REPO_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            # Kill the victim while it is still sleeping, so the master's RPC to
            # it fails and the task has to be redirected.
            time.sleep(self.SLEEP_TIME / 2.0)
            victim.kill()
            victim.wait(timeout=10)
            self.assertFalse(
                port_is_open(port),
                f"{kind} {index + 1} on port {port} is still listening after being killed",
            )
            stdout, _ = master.communicate(timeout=300)
        return master.returncode, stdout

    def test_a_killed_mapper_is_redirected_and_the_run_finishes(self):
        returncode, stdout = self._run_with_one_worker_killed(
            "mapper", 1, num_mappers=3, num_reducers=2, num_centroids=2
        )
        self.assertEqual(returncode, 0, f"the master did not survive a dead mapper:\n{stdout}")
        self.assertIn("Redirecting to next mapper", stdout)
        dump = read_master_dump()
        self.assertIn("Failed like Scenario 2, redirecting to next mapper", dump)
        for redirect_id in re.findall(r"redirecting to next mapper with id: (-?\d+)", dump):
            self.assertGreater(int(redirect_id), 0, "the redirect logged a non-positive mapper id")
        self.assertEqual(len(read_points(FINAL_CENTROIDS_PATH)), 2)

    def test_a_killed_reducer_is_redirected_and_the_run_finishes(self):
        returncode, stdout = self._run_with_one_worker_killed(
            "reducer", 1, num_mappers=3, num_reducers=3, num_centroids=3
        )
        self.assertEqual(returncode, 0, f"the master did not survive a dead reducer:\n{stdout}")
        self.assertIn("Redirecting to next reducer", stdout)
        dump = read_master_dump()
        self.assertIn("Failed like Scenario 2, redirecting to next reducer", dump)
        for redirect_id in re.findall(r"redirecting to next reducer with id: (-?\d+)", dump):
            self.assertGreater(int(redirect_id), 0, "the redirect logged a non-positive reducer id")
        self.assertEqual(len(read_points(FINAL_CENTROIDS_PATH)), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
