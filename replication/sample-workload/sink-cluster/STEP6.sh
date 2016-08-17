#!/bin/bash 

echo "Count rows in 'test1' and 'test2' tables ..."

hbase shell << ... 2> /dev/null

count 'test1'

count 'test2'

exit
...

