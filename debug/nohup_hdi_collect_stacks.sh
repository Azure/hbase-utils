#!/bin/bash 

wget https://raw.githubusercontent.com/Azure/hbase-utils/master/debug/hdi_collect_stacks.sh -O /tmp/hdi_collect_stacks.sh

chmod +x /tmp/hdi_collect_stacks.sh

nohup bash /tmp/hdi_collect_stacks.sh $@ &
