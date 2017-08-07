#/bin/bash
result=$(echo "rmr /hbase/region-in-transition"|hbase zkcli 2>&1 | grep "Node does not exist:")
if [[ -z $result ]]; then
    echo "Regions stuck in transition successfully removed, running hbck to confirm"
    hbckresult=$(hbase hbck -ignorePreCheckPermission | grep "0 inconsistencies detected")
    if [[ ! -z $hbckresult ]]; then
        echo "HBase is in good health"
    else
        echo "Could not confirm hbase status; please check by running hbck manually"
    fi
else
    result=$(echo "rmr /hbase-unsecure/region-in-transition"|hbase zkcli 2>&1 | grep "Node does not exist:")
    if [[ -z $result ]]; then
        echo "Regions stuck in transition successfully removed, running hbase hbck to confirm"
        hbckresult=$(hbase hbck -ignorePreCheckPermission | grep "0 inconsistencies detected")
        if [[ ! -z $hbckresult ]]; then
            echo "HBase is in good health"
        else
            echo "Could not confirm hbase status; please check by running hbck manually"
        fi        
    else
        echo "No entries in found corresponding to region-in-transition znode; please contact support if you need further assistance"
    fi
fi
