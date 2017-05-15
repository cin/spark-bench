# spark-bench
## Benchmark Suite for Apache Spark

[![Build Status](https://travis-ci.org/ecurtin/spark-bench.svg?branch=master)](https://travis-ci.org/ecurtin/spark-bench)
[![codecov](https://codecov.io/gh/ecurtin/spark-bench/branch/master/graph/badge.svg)](https://codecov.io/gh/ecurtin/spark-bench)


### Current Spark and spark-bench version: 2.1.0

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
# Table of Contents

- [Installation](#installation)
- [Usage](#usage)
  - [Terms](#terms)
  - [Command Line Usage](#command-line-usage)
    - [spark-bench generate-data](#spark-bench-generate-data)
    - [Multiple Arguments for Workload Suite](#multiple-arguments-for-workload-suite)
    - [Parallel Runs](#parallel-runs)
    - [Multi-Run](#multi-run)
- [Configuration File](#configuration-file)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Installation

1. Grab the latest release from here: <https://github.com/ecurtin/spark-bench/releases/latest>.
2. Unpack the tarball using `tar -xvzf`.
3. `cd` into the newly created folder.
4. Modify `SPARK_HOME` and `SPARK_MASTER_HOST` in `bin/spark-bench-env.sh` to reflect your environment. 
5. Start using spark-bench!

Alternatively, you can also clone this repo and build using `sbt dist`, then set the environment variables as above.

## Usage

### Terms

- `data generator` or `generator`: spark-bench allows the user to easily generate data and store it locally, in HDFS, or in remote storage such as S3.
- `workload`: a single task that gets run on a Spark cluster. A workload could be running KMeans over a set of data, or creating partitions and having them sleep, or doing SQL queries, or...
- `suite`: a suite is a grouping of one or many workloads that will output their results in a common place. Workloads within a suite can be run serially or in parallel.
- `context` or `Spark context`: currently spark-bench only supports running suites in a single spark context with a single set of Spark parameters. In the future, it will support running
multiple contexts. This will allow a user to, for example, run a KMeans benchmark over different values of K and different amounts of executor memory.

### Command Line Usage

The spark-bench CLI lets you run a single suite of workloads of the same type. `kmeans-example.sh` show several examples of how to take it for a test drive.

spark-bench has two major subcommands: `generate-data` and `workload`. 

#### spark-bench generate-data

spark-bench wraps a number of data generators built into Spark as well as several custom generators. Each generator can be run in a configurable manner through the CLI.
For example, `bin/spark-bench.sh generate-data kmeans -r 5 -c 5 -o /tmp/spark-bench-demo/kmeans-dataset-1 --output-format csv` will run the KMeans data generator and will output
5 rows and 5 columns worth of data with the default KMeans parameters.

To see a sample of your data before you create gigs and gigs worth, specify a very small number of rows (recommend no more than 20) and specify `-o console` to see the
output in your terminal. If you really want to specify `-r 10000000 -c 5000 -o console`, you can, but know that under the hood it's doing a `dataframe.collect` so it's 
gonna break in the same way that a large `collect` would break a normal Spark job.


The rewrite is currently in MVP stage and only supports KMeans data generation and workload.

See `bin/kmeans-example.sh` and run it from the spark-bench directory to see it in action.

`spark-bench --help` shows the help menu.

`spark-bench generate-data --help` or `spark-bench workload --help` will give you more info on the available data generators, workloads, and their respective options.
 
#### Multiple Arguments for Workload Suite

You can have single arguments or a space-separated list of arguments that will be taken in as a Sequence.
This is best understood through example:

`./bin/spark-bench.sh workload -i /tmp/input-data1 -o /Users/ecurtin/Desktop/test-results.csv kmeans -k 2`
will run kmeans 1 time over the /tmp/input-data1 set and a k value of 2.

`./bin/spark-bench.sh workload -i /tmp/input-data1 /tmp/input-data2 -o /Users/ecurtin/Desktop/test-results.csv kmeans -k 2`
will run kmeans 2 times, once over the /tmp/input-data1 set with a k value of 2, and another time over the /tmp/input-data2 set with a k-value of 2.

`./bin/spark-bench.sh workload -i /tmp/input-data1 /tmp/input-data2 -o /Users/ecurtin/Desktop/test-results.csv kmeans -k 2 32`
will run kmeans 4 times with all combinations of /tmp/input-data1, /tmp/input-data2, k = 2, k = 32.

#### Parallel Runs

Workloads in a suite can be run serially or in parallel across a SparkSession. By default, workloads will be run one after the other. 
Specify `--parallel` to make your workloads run in parallel on the SparkSession.

`./bin/spark-bench.sh workload -i /tmp/input-data1 /tmp/input-data2 -o /Users/ecurtin/Desktop/test-results.csv --parallel kmeans -k 2 32`
will run kmeans 4 times over the two datasets and k values, but unlike the example above it will launch all four variations of the workload in parallel in one SparkSession.

#### Multi-Run

You can run a suite of workloads multiple times by specifying the `-n` argument. 
This is especially useful for benchmark timings where multiple runs are necessary to correct for system noise.

`./bin/spark-bench.sh workload -i /tmp/coolstuff1 -o console -n 5 kmeans -k 2`
will run kmeans once over one dataset with one value of k, and will repeat that workload 5 times and output all 5 results

`./bin/spark-bench.sh workload -i /tmp/coolstuff1 /tmp/coolstuff2 -o console -n 5 kmeans -k 2 32`
creates a suite of 4 kmeans workloads that will be run serially. By specifying `-n 5`, this suite will be run 5 times.

## Configuration File

You can do everything available in the CLI and more by running spark-bench through a configuration file.