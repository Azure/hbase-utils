#!/bin/bash

echo "Creating tables test1 and test2..."

hbase shell << ... 2> /dev/null
	disable 'test1'
	drop 'test1'
	create 'test1', 'family'

	disable 'test2'
	drop 'test2'
	create 'test2', 'family'
	exit
...


echo "Inserting 100 rows in test1..."

for i in `seq 1 100`; do echo put \'test1\', \'row$i\', \'family:c1\', \'value$i\'; done > /tmp/workload.txt
echo "exit" >> /tmp/workload.txt
echo "" >> /tmp/workload.txt

hbase shell /tmp/workload.txt > /dev/null 2>&1

hbase shell << ... 2> /dev/null
	count 'test1'
	count 'test2'
	exit
...

