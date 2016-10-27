#!/bin/bash 

wget https://raw.githubusercontent.com/Azure/hbase-utils/master/debug/hdi_collect_stacks.sh -O /tmp/hdi_collect_stacks.sh

chmod +x /tmp/hdi_collect_stacks.sh

nohup /tmp/hdi_collect_stacks.sh $@ > /var/log/hbase/hdi_collect_stacks.log 2>&1
