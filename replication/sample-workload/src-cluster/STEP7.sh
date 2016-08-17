#!/bin/bash

echo "Replication info..."

hbase shell << ... 2> /dev/null
	list_peers
	list_replicated_tables
	status 'replication'
	exit
...


