#!/bin/bash

echo "Enabling replication on all the tables..."
sleep 3
bash ./enable_replication.sh -s hbrepl1 -d hbrpl2 -sp 'Had00p\!123' -dp Had00p\!123 -t "test1;test2"
