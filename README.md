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

`run.py` depends on the `cmd.exe` `start` builtin, so run the processes by hand
(here `M=3`, `R=2`, `K=2`, 10 iterations):

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

`num_reducers` is clamped down to `num_centroids`: a reducer needs at least one
centroid key to be useful.

`sleep_seconds` is the delay a mapper or reducer inserts before doing any work.
It prints `SLEEPING... Please Terminate` and exists purely to give you a window
in which to force-stop the process and watch the master reassign the task. Use
`0` for a normal run.

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
directories it needs on startup.

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
