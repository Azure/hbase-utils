#!/bin/bash 

echo "List all the replica tables and count of rows... "

hbase shell << ... 2> /dev/null

list

desc 'test1'

desc 'test2'

count 'test1'

count 'test2'

exit
...

