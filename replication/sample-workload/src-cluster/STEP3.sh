#!/bin/bash

echo "Enabling replication on all the tables..."
sleep 3

echo "[INFO] Downloading https://raw.githubusercontent.com/Azure/hbase-utils/master/replication/hdi_enable_replication.sh to /tmp directory."

wget https://raw.githubusercontent.com/Azure/hbase-utils/master/replication/hdi_enable_replication.sh -O /tmp/hdi_enable_replication.sh

chmod +x /tmp/hdi_enable_replication.sh

bash /tmp/hdi_enable_replication.sh -s hbrepl1 -d hbrpl2 -sp 'Had00p\!123' -dp Had00p\!123 -t "test1;test2" -cp

