#!/usr/bin/env bash

docker run -it --name pulsar -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data apachepulsar/pulsar:2.4.0 bin/pulsar standalone

docker exec -it pulsar /pulsar/bin/pulsar-admin tenants create test
docker exec -it pulsar /pulsar/bin/pulsar-admin namespaces create test/rawdata
docker exec -it pulsar /pulsar/bin/pulsar-admin namespaces set-retention --size -1 --time -1 test/rawdata

# Pulsar SQL commands
docker exec -it pulsar apt-get update && apt-get install less
docker exec -it pulsar touch /root/.presto_history
docker exec -it pulsar /pulsar/bin/pulsar sql-worker start

# Test Pulsar SQL (Presto) using interactive shell
#/pulsar/bin/pulsar sql
