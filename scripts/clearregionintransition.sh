#/bin/bash
echo "ls /hbase/region-in-transition"|hbase zkcli
result=$(echo "rmr /hbase/region-in-transition"|hbase zkcli 2>&1 | grep "Node does not exist:")
echo $result
if [[ -z $result ]]; then
    echo "Regions stuck in transition successfully removed, please retry hbase hbck command"
else
   echo "ls /hbase-unscure/region-in-transition"|hbase zkcli
    result=$(echo "rmr /hbase-unsecure/region-in-transition"|hbase zkcli 2>&1 | grep "Node does not exist:")
    if [[ -z $result ]]; then
        echo "Regions stuck in transition successfully removed, please retry hbase hbck command"
    else
        echo "Operation failed; please contact support if you need more assistance"
    fi
fi
