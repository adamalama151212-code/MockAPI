# Lab 3 - streaming and incremental ingestion

Lab 2 loaded three big files into bronze once. This lab is about the two ways data actually keeps
arriving: as files that pile up in a folder, and as events pushed through a broker. Both land in
the same `adam151212_bronze` schema, so the medallion layer does not change here. What changes is
how the data gets in.

Two pipelines, completely separate from each other:

```
files:   generate_batch()    -> 1000 json files in ADLS  -> Auto Loader -> raw_events_stream_bronze
events:  eventhub_producer   -> Event Hub adam151212-ehv -> Spark Kafka -> eventhub_events_bronze
```

## Coordinates

- Catalog `dbr_dev`, schema `adam151212_bronze`, container `adamalama` in `dlspl21databricks`
- Volumes: external `landing_stream` (source files), managed `checkpoints`
- Event Hubs namespace `evhpl24databricks02`, hub `adam151212-ehv` (2 partitions, 1h retention),
  consumer group `adam151212-cg`
- Secret scope `adam151212_scope`, key `eventhub-cs-ns`
- Cluster: shared all purpose `GP1`, same choice as Lab 2 (a job cluster would spin up new compute
  for something that is already running)

## Files

| file | what it is |
|---|---|
| `autoloader_lab_3.ipynb` | file ingestion, schema evolution, triggers, reloading |
| `eventhub_producer.ipynb` | sends telemetry to the Event Hub |
| `eventhub_consumer.ipynb` | reads it back and writes to bronze, this one runs as a job |

## Auto Loader

The lab asks for around 1000 files in storage. Uploading that many from a laptop would take hours,
so the notebook makes them on the cluster instead: read the bronze table from Lab 2, drop the
ingestion metadata to get the original records back, write it out across 1000 partitions.

Every run writes into its own `batch_ts=<timestamp>` folder. Spark gives each part file a random
id, so without that folder there would be no way to tell later which generation a row came from.
It also matters because Auto Loader tracks files by full path, so a repeated name would be skipped
as already seen. As a bonus the folder shows up as a partition column.

Results:

- 1000 files, about 160 MB, loaded into `raw_events_stream_bronze` as 1 004 010 rows
- `maxFilesPerTrigger=100` split that into 10 micro batches plus one empty batch at the end where
  `availableNow` checks that nothing new arrived
- the first batch took roughly twice as long as the rest, which seems to be the cost of inferring
  the schema and listing the directory for the first time
- `DESCRIBE HISTORY` shows one `STREAMING UPDATE` commit per micro batch

### Schema evolution

Added a `priority` column to a new generation of 50 000 records to simulate the source system
changing. With `schemaEvolutionMode=addNewColumns`, which is the default, the stream fails with
`UnknownFieldException`. The error itself says `can be fixed by an automatic retry: true`. On the
way out it writes the wider schema to `schemaLocation`, so `_schemas` now holds version 0 and
version 1. Running exactly the same call again goes through, no code change involved.

After that the table has 1 054 010 rows. Old rows have `priority` as null, new ones have the value,
so nothing had to be rewritten.

The other mode, `rescue`, was tried on a small separate table. There the stream never fails, but
`priority` does not become a column either. It ends up inside `_rescued_data` as JSON text, along
with the path of the file it came from. So the data survives, but you have to parse a string to get
at it, and nothing tells you a new field appeared.

The way I read the trade off: `addNewColumns` costs one failed run and gives clean columns,
`rescue` keeps the pipeline up and hides the problem until someone looks.

### Small files

Ten micro batches meant eleven files in the table. Turned on `optimizeWrite` and `autoCompact` and
ran `OPTIMIZE`, which brought it down to one file for the same row count. Worth noting that this
does not free storage right away, the old files stay for time travel until `VACUUM` removes them.

This is the output side of the small files problem. The input side is what `maxFilesPerTrigger`
already handles.

### Triggers

Same 20 files, same `maxFilesPerTrigger=5`:

| trigger | batches | rows |
|---|---|---|
| `availableNow` | 4 | 20 000 |
| `once` | 1 | 20 000 |

Both read everything, so correctness is the same. The difference is that `once` ignores the file
limit and does it in a single batch, which is presumably why it is deprecated. With 1000 files that
would be one very large batch. There is also a recovery angle: four batches are four commits, so a
failure halfway through only costs the unfinished part.

`processingTime` is not run in the notebook, only described, because it never stops on its own and
the cluster is shared.

### Reloading

Deleting the checkpoint on its own doubled the demo table from 4000 to 8000 rows. The stream forgot
which files it had seen and read them all again, while the table still held the previous load. What
makes this unpleasant is that nothing fails. The job stays green and the data is wrong.

Deleting the checkpoint together with truncating the table gave back exactly 4000 rows, asserted in
the notebook. So the rule is that the checkpoint and the table are one pair. Wipe both or neither.

## Event Hub

The producer takes 20 000 records from bronze, adds a `producer` field and a send timestamp, turns
each row into one JSON string, and sends them in batches of about 1 MB. The hub is shared with the
whole group, so without stamping the events there would be no way to separate ours from everyone
else's. It sent 20 000 events in 5 batches in 2.8 seconds.

No partition key is set. We do not need ordering per device, and without a key the batches spread
across both partitions, which the consumer can then read in parallel.

The consumer uses the plain `kafka` source. Event Hubs is not Kafka but it exposes a Kafka
compatible endpoint on port 9093, so no extra library is needed. It filters on `producer` and
writes to `eventhub_events_bronze` together with the broker metadata (partition, offset, enqueued
time), which is the only way to prove later where a row came from.

Result: 20 000 rows, matching what was sent. Partition 1 got 11 149 events and partition 0 got
8851, offsets running from zero without gaps, which is the round robin working.

`payload` is kept as a JSON string rather than parsed into columns. The shape differs per event
type, detections carry `label`, `box` and `confidence` while health events carry `status` and
`ping_ms`, so one fixed structure would not fit both. Splitting it feels like a silver concern.

No UDF anywhere. `from_json` is a built in that runs inside the JVM, while a Python UDF would move
every row into a Python process and back for parsing that Spark already does.

## Job

`adam151212_eventhub_ingestion`, a single notebook task `ingest_eventhub` pointing at
`LAB3/eventhub_consumer`, running on `GP1`. The run history is in the Runs tab of the job.

Settings that matter:

- task parameters matching the widget names, set on the task and not at job level, otherwise the
  notebook sees empty widgets and the assert stops it
- retries 1 with a delay, because one immediate retry after a connection hiccup is useful while
  three instant ones are just the same error three times
- max concurrent runs 1, since two runs would fight over the same checkpoint
- interval trigger, every hour

The job stays on `availableNow` rather than a continuous stream. A continuous stream needs the
cluster alive the whole time, while this wakes up, drains whatever is waiting, and stops. For
bronze ingestion the extra latency seems like a fair trade on shared infrastructure.

The schedule is paused after the successful run. The producer is triggered by hand, so an hourly
job would mostly wake up, find nothing, and shut down again. The configuration is there and the run
history shows it worked.

## Exactly once and at least once

The file source can always read the same file again, and each micro batch is one atomic Delta
commit with its batch id recorded. On restart Spark sees the batch is already committed and skips
it, so the file pipeline is exactly once end to end.

A broker only remembers a position. If a consumer dies after processing but before that position is
saved, the same events come back. That is at least once. Structured Streaming narrows it by keeping
offsets in its own checkpoint and committing them with the data, so this pipeline effectively gets
exactly once. What it cannot fix is the producer resending after a network timeout, since that
retry is invisible on our side.

That is what `event_id` is for. If duplicates mattered downstream the consumer could add:

```python
.withWatermark("enqueued_ts", "10 minutes").dropDuplicates(["event_id", "enqueued_ts"])
```

The watermark is there to bound how long the deduplication state is kept, otherwise it grows
forever.

## Things that went differently than planned

**Key Vault disappeared.** The connection string first went into `adamalamakv3`, a vault created
in access policy mode specifically so that a Key Vault backed scope would work, which was the one
thing left unfinished in Lab 2. Two days later `dbutils.secrets.get` failed with
`UnknownHostException` on the vault address, meaning it no longer existed. Several scopes belonging
to other people in this workspace point at vaults that are gone in the same way, so this looks like
routine cleanup of student resources. Moved the secret to `adam151212_scope`, which is Databricks
backed and does not depend on an Azure resource that can be removed.

**Hub level SAS does not work over Kafka.** The first policy was created on the event hub itself.
The producer worked fine with it because the SDK uses AMQP, but the consumer kept failing with
`UnknownTopicOrPartitionException`. A Kafka client asks the namespace for topic metadata on
connect, so the policy has to be at namespace level. The namespace level connection string also has
no `EntityPath` in it, which is a quick way to check you copied the right one.

**Typo in the hub name.** The hub is `adam151212-ehv` and I had typed `adam151212-evh` in the
widget, which cost some time before I spotted it.
