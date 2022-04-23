#!/bin/bash
docker run --name scylla-node-1 \
  -p "9042:9042" \
  -v "scylla-data-1:/var/lib/scylla" \
  -v "$PWD/scylla.yaml:/etc/scylla/scylla.yaml" \
  -v "$PWD/cassandra-rack-dc.properties.dc1:/etc/scylla/cassandra-rackdc.properties" \
  -d scylladb/scylla:latest
