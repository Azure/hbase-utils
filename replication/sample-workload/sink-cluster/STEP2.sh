#!/bin/bash 

echo "Dropping all tables shown below... "

curl -u admin:Had00p\!123 -G "https://hbrpl2.azurehdinsight.net/hbaserest/" 2> /dev/null

echo "" > /tmp/workload.txt

for user_table in `curl -u admin:Had00p\!123 -G "https://hbrpl2.azurehdinsight.net/hbaserest/"`
do
cat << ... >> /tmp/workload.txt
	disable '$user_table'
	drop 	'$user_table'
...
done

cat << ... >> /tmp/workload.txt
	list
	exit
...

echo "Doing following operations ..."
cat /tmp/workload.txt

hbase shell /tmp/workload.txt 2> /dev/null
