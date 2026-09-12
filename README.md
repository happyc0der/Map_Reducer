# MapReduce K-Means

A MapReduce framework built from scratch with gRPC, used to run K-Means
clustering on a 2-D dataset. CSE530 Distributed Systems (Winter 2024),
Assignment 3 — see [Assignment.pdf](Assignment.pdf) for the original spec.

Everything runs on one machine, but the master, every mapper and every reducer
is a **separate process** with its own port, and each writes its intermediate or
final output to its own directory, as the assignment requires.

## Components

| File | Role |
| --- | --- |
| [Master.py](Master.py) | Drives the iteration loop: input split, centroid compilation, convergence check, fault handling |
| [Mapper.py](Mapper.py) | Serves `Map` (map + partition) and `Reduce` (hands partitions to a reducer) |
| [Reducer.py](Reducer.py) | Serves `StartReduce` (shuffle, sort, reduce) and `returnCentroid` |
| [run.py](run.py) | Windows launcher that spawns all of the above in separate windows |
| [run_posix.py](run_posix.py) | macOS/Linux launcher; also the `Cluster` helper the tests reuse |
| [test_kmeans.py](test_kmeans.py) | Test suite, including a from-scratch reference K-Means to check the result against |
| [RPC/mapreduce.proto](RPC/mapreduce.proto) | The single gRPC service shared by all three programs |
| `mapreduce_pb2*.py` | Stubs generated from the `.proto`; committed, regenerate only after editing the proto |
| [check.ipynb](check.ipynb) | Scratch notebook that plots the input points and the final centroids |

### Ports

Derived from the master port `4040`:

| Process | Port |
| --- | --- |
| Master | 4040 (does not listen; used as the base for the arithmetic) |
| Mapper *i* (1-indexed) | `4040 + i` → 4041, 4042, … |
| Reducer *i* (1-indexed) | `4040 - i` → 4039, 4038, … |

On the command line the mapper and reducer indices are **0-based**
(`python Mapper.py 0 0` is mapper id 1 on port 4041).

## One iteration

```
                 (1) Map                       (2) StartReduce
   Master ───────────────────────► Mapper       Master ──────────────────► Reducer
          ◄─────────────────────── (map +              ◄────────────────── (shuffle,
              status OK / FAILED    partition)              ok = 1          sort, reduce)

                 (3) Reduce                     (4) returnCentroid
   Reducer ──────────────────────► Mapper       Master ──────────────────► Reducer
           ◄────────────────────── (reads its           ◄────────────────── (updated
              key → [points]        partitions)          ok + centroids      centroids)
```

1. **Input split** (master) — the input is a single file, so the master only
   computes an index range `[begin, end)` per mapper and never ships data
   (Scenario 1 of the spec). The last mapper absorbs the remainder.
2. **Map + partition** (mapper) — the mapper reads `Data/Input/points.txt`
   itself, emits `(nearest centroid index, point)` for each point in its range,
   and writes each pair to `partition_(key % R) + 1.txt` inside its own
   directory. All pairs with the same key therefore land in the same partition,
   and there are `M * R` partitions in total.
3. **Shuffle & sort + reduce** (reducer) — reducer *i* owns partition *i*. It
   pulls that partition from **every** mapper over gRPC (it never reads the
   intermediate files directly), groups the values by key, averages each group,
   and appends `<centroid id> <x> <y>` lines to `Data/Reducers/R<i>.txt`.
4. **Centroid compilation** (master) — the master asks every reducer for its
   centroids, keys them by centroid id, sorts by id, and uses the result as the
   input of the next iteration.
5. **Convergence** — the loop stops early once every centroid moves by less than
   `EPSILON = 0.0001` on both axes, otherwise after `num_iterations` rounds. The
   final list is written to `Data/centroids.txt`.

## Running it

Requires Python 3.10+ and `grpcio`:

```bash
pip install grpcio
```

Start the mappers and reducers **first**, then the master. The mappers and
reducers are long-lived servers that stay up across iterations; the master exits
when the run finishes.

### Windows

```bash
python run.py <num_mappers> <num_reducers> <num_centroids> <num_iterations> <sleep_time>
```

`run.py` clears the generated directories, opens one `cmd.exe` window per
process and leaves each window open so its log stays readable. Close a window to
kill that process. Point `virtualenv_python` at the top of `run.py` at the
interpreter you want to use.

### macOS / Linux

`run.py` depends on the `cmd.exe` `start` builtin, so use `run_posix.py`, which
takes the same arguments:

```bash
python run_posix.py <num_mappers> <num_reducers> <num_centroids> <num_iterations> <sleep_time>
```

For example, `python run_posix.py 3 2 2 10 0` runs three mappers and two reducers
over two centroids for up to ten iterations. It clears the generated
directories, refuses to start if a worker from an earlier run is still holding a
port, waits until every worker is really listening, runs the master in the
foreground, and shuts the workers down when the master exits — including on
Ctrl-C. Worker output goes to `logs/mapper_<id>.log` and `logs/reducer_<id>.log`.

It works on Windows too; you just get log files instead of one window per
process.

To start the processes by hand instead (here `M=3`, `R=2`, `K=2`, 10 iterations):

```bash
rm -rf Data/Mappers Data/Reducers Data/Dump
for i in 0 1 2; do python Mapper.py "$i" 0 & done
for i in 0 1;   do python Reducer.py "$i" 0 & done
sleep 2
python Master.py 3 2 2 10
```

Kill the backgrounded mappers and reducers when the master is done.

### Arguments

| Program | Arguments |
| --- | --- |
| `Master.py` | `<num_mappers> <num_reducers> <num_centroids> <num_iterations>` |
| `Mapper.py` | `<mapper_index (0-based)> <sleep_seconds>` |
| `Reducer.py` | `<reducer_index (0-based)> <sleep_seconds>` |
| `run.py` | `<num_mappers> <num_reducers> <num_centroids> <num_iterations> <sleep_time>` |
| `run_posix.py` | same as `run.py` |

`num_reducers` is clamped down to `num_centroids`: a reducer needs at least one
centroid key to be useful.

`sleep_seconds` is the delay a mapper or reducer inserts before doing any work.
It prints `SLEEPING... Please Terminate` and exists purely to give you a window
in which to force-stop the process and watch the master reassign the task. Use
`0` for a normal run.

## Tests

```bash
python -m unittest -v test_kmeans
```

There is no test framework to install — `unittest` is in the standard library,
and the tests need nothing beyond the `grpcio` the pipeline already requires.
Each test starts its own mappers and reducers through `run_posix.Cluster` and
shuts them down afterwards, so **nothing may already be listening on the
mapper/reducer ports**; the suite refuses to start rather than quietly talking to
a stale worker. The 62 tests take a few seconds in total, because the sample
input is only 25 points.

The fault-injection tests have to wait inside a worker's sleep window to kill it,
so they take about 80 seconds and are skipped unless you ask for them:

```bash
RUN_FAULT_TESTS=1 python -m unittest -v test_kmeans
```

A single class, or a single test:

```bash
python -m unittest -v test_kmeans.TestOutput_3M_2R_3K
python -m unittest -v test_kmeans.TestEndToEnd.test_matches_reference_kmeans
```

### What is checked

The central test is `TestEndToEnd.test_matches_reference_kmeans`. It runs the
distributed pipeline, then replays the same K-Means from the same randomly
sampled starting centroids using `reference_kmeans` — a plain single-process
implementation written from scratch in the test file — and asserts the two agree
to within 1e-6. That turns "it produced some centroids" into "it produced the
right centroids". The reference deliberately mirrors `Master.py`'s loop,
including that ties go to the lowest centroid index and that a converging run
writes the centroids from *before* the final update.

The suite is organised around the assignment's evaluation criteria:

| Rubric item | Tests |
| --- | --- |
| Test the master with different numbers of mappers, reducers, centroids and iterations | `OUTPUT_CONFIGURATIONS` drives the whole output suite over 1x1, 3x2, 4x3 and 8x2, with K of 2 and 3, and both a single iteration and converging runs; `TestEndToEnd` covers the same spread end to end |
| Verify the outputs of mappers and reducers for different configurations | Every test in `OutputVerification` runs once per configuration, as `TestOutput_1M_1R_2K`, `TestOutput_3M_2R_3K`, `TestOutput_4M_3R_3K` and `TestOutput_8M_2R_2K` |
| Input split, and the master shipping no data | `test_each_mapper_only_writes_its_own_split`, `test_every_input_point_is_mapped_exactly_once`, `TestProtocol.test_the_map_request_carries_indices_not_data_points` |
| Map output key/value, written to the mapper's own directory | `test_every_point_is_keyed_to_its_nearest_centroid`, `test_the_directory_layout_matches_the_specification` |
| Partition: R per mapper, M×R in total, same key to the same partition | `test_there_are_m_times_r_partitions`, `test_a_key_only_ever_lands_in_one_partition` |
| Reducers pulling their input over gRPC instead of reading the intermediate files | `TestProtocol.test_every_reducer_pulls_from_every_mapper_over_grpc` |
| Reduce output is the updated centroid, in the reducer's own directory | `test_a_reduced_centroid_is_the_mean_of_its_group`, `test_reducer_output_has_well_formed_lines_owned_by_that_reducer` |
| Centroid compilation into a single file, and random initial centroids from the input | `test_final_centroids_are_compiled_from_the_reducer_output`, `TestEndToEnd.test_the_initial_centroids_are_sampled_from_the_input` |
| Fault tolerance, Scenario 1 (self-reported failure) | `TestScenarioOneFailures` |
| Fault tolerance, Scenario 2 (force-stopped process) | `TestFaultTolerance` |
| Required output: iteration number, gRPC calls, SUCCESS/FAILURE per worker, and the centroids of each iteration including the initial ones | `TestLogging` |

`TestReferenceImplementation` tests the reference K-Means itself against a
hand-worked example, so a broken reference cannot hide a bug in the pipeline.

### Skips you may see

* **Scenario 1 not observed** — the injected failure is random (5% per call), so
  `TestScenarioOneFailures` repeats the run up to 12 times and skips if it never
  fires.
* **Empty cluster** — if the random starting centroids leave one centroid with no
  points assigned to it, the master cannot finish (see the known limitations
  below), so the affected test skips with that explanation rather than reporting
  a failure.

## Fault tolerance

Both failure scenarios from the spec are handled, and every failure is recorded
in the dump files.

| Scenario | How it is simulated | How the master reacts |
| --- | --- | --- |
| **1** — the task fails but the process is alive | A mapper answers `Map` with `status = "FAILED"`, or a reducer answers `returnCentroid` with `ok = 0`, with probability 5% (`FAILURE_FREE_PROBABILITY = 0.95` in `Mapper.py` / `Reducer.py`) | Retries the same worker until it succeeds |
| **2** — the process is gone | Start the worker with a non-zero `sleep_seconds` and force-stop it during the sleep. Close the window on Windows; on macOS/Linux prefer `kill` — a Ctrl-C in a shared terminal reaches every process in the group | The RPC raises, and the task is reassigned: a map split goes to the next mapper, a reduce partition to a random surviving reducer. The replacement is told to **append** so it keeps the work it already owns |

## Data layout

```
Data/
├─ Input/
│  └─ points.txt            # input, one "x,y" per line (committed)
├─ Mappers/                 # generated; M directories × R partitions
│  ├─ M1/
│  │  ├─ partition_1.txt    # "<centroid id>,<x>,<y>" per line
│  │  └─ partition_2.txt
│  └─ M2/ …
├─ Reducers/                # generated
│  ├─ R1.txt                # "<centroid id> <x> <y>" per line, appended per iteration
│  └─ R2.txt
├─ Dump/                    # generated logs, one per process
│  ├─ master_dump.txt
│  ├─ M1_dump.txt
│  └─ R1_dump.txt
├─ initial_centroids.txt    # the randomly sampled starting centroids
└─ centroids.txt            # the final centroids
```

`Data/Mappers`, `Data/Reducers` and `Data/Dump` are generated output and are
gitignored, so they will not exist in a fresh clone — each process creates the
directories it needs on startup. `run_posix.py` also writes worker stdout to a
gitignored `logs/` directory.

The master prints the iteration number, every gRPC call it makes, each
SUCCESS/FAILURE response and the centroids after each iteration; the same
information is appended to `Data/Dump/master_dump.txt`, and the mappers and
reducers keep their own dump files.

## Known limitations

* `Data/Reducers/R<i>.txt` is only ever appended to, so it accumulates one block
  of centroids per iteration. This is harmless because the master keys the
  values it reads back by centroid id and the newest line for a key wins, but
  the file is a history rather than a snapshot of the last iteration.
* `COLLECTED_REDUCE_ACKS` in `Master.py` is likewise never cleared between
  iterations, for the same reason and with the same caveat.
* A split reassigned after a Scenario 2 failure can still be lost. The master
  sends all `Map` calls concurrently, so if the replacement mapper has not
  finished its *own* `Map` call yet, that call runs with `append = 0`, clears the
  partition directory and wipes the split that was just handed to it. This is
  easy to reproduce with a non-zero `sleep_seconds`: kill a mapper during its
  sleep and the reassigned points disappear from the partition files.
* If a mapper reports a Scenario 1 failure for a split that had already been
  reassigned to it after a Scenario 2 failure, the retry starts over with
  `append = 0` and clears that mapper's partition directory, dropping the split
  it originally owned for that iteration.
* A reducer that dies between `StartReduce` and `returnCentroid` is logged as a
  Scenario 2 failure but its partition is not reassigned at that point, so its
  centroids are missing from that iteration.
* The number of mappers and reducers is fixed for the whole run; the master does
  not spawn replacements, it only redirects work to workers that are already up.
