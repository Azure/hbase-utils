#!/bin/bash 

echo "Dropping test1 and test2 tables... "

hbase shell << ... 2> /dev/null
disable 'test1'
drop    'test1'
disable 'test2'
drop    'test2'
list
exit
...



