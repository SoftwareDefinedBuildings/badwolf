## Automatic AWS

This is a set of convenience scripts to start up an arbitrary number of EC2
instances.

* `aws.ini` should contain all relevant information about the AWS intances

* `configure_aws.py` starts the instances and creates `ips.csv` which is a file
  that contains a list of all started instances

Currently, for each of the benchmarks, we assume 1 instance for the server and
N instances for the client.

We will be using spot instances for the benchmarking.
