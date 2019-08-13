#!/usr/bin/env bash
docker run -it --name pulsar -p 6650:6650 -p 8080:8080 -p 8081:8081 -v $PWD/data:/pulsar/data apachepulsar/pulsar:2.4.0 bin/pulsar standalone
