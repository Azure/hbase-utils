#!/bin/bash

echo "Insert 100 rows into 'test1' and 'test2' each..."

for i in `seq 101 200`; do echo put \'test1\', \'row$i\', \'family:c1\', \'value$i\'; done > /tmp/workload.txt

for i in `seq 1 100`; do echo put \'test2\', \'row$i\', \'family:c1\', \'value$i\'; done >> /tmp/workload.txt

echo "exit" 	>> /tmp/workload.txt
echo "" 	>> /tmp/workload.txt

hbase shell /tmp/workload.txt > /dev/null 2>&1

hbase shell << ... 2> /dev/null
	count 'test1'
	count 'test2'
	exit
...


