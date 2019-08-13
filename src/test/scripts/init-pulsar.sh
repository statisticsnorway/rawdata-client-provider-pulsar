#!/usr/bin/env bash

# Create tenant and namespace
docker exec -it pulsar /pulsar/bin/pulsar-admin tenants create test
docker exec -it pulsar /pulsar/bin/pulsar-admin namespaces create test/rawdata
docker exec -it pulsar /pulsar/bin/pulsar-admin namespaces set-retention --size -1 --time -1 test/rawdata

# Pulsar SQL commands
docker exec -it pulsar apt-get update && docker exec -it pulsar apt-get install less
docker exec -it pulsar touch /root/.presto_history
docker exec -it pulsar /pulsar/bin/pulsar sql-worker start

# Test Pulsar SQL (Presto) using interactive shell
#docker exec -it pulsar /pulsar/bin/pulsar sql
