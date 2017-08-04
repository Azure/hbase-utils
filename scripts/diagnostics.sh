#/bin/bash

ismasterrunning() {
    hbaseStatus=$(echo "status"|hbase shell 2>&1 | grep "1 active master")
    if [[ ! -z $hbaseStatus ]]; then
        #0 = true
        return 0
    else
        #1 = false
        return 1
    fi
}

cleanuppendingrenamefiles() {
    echo "Looking for hbase folders with pending rename files that might be causing HMaster start failures..."

    for folder in '/hbase/data' '/hbase/.tmp/data' '/hbase/WALs'
    do
        echo "Processing folder $folder"
        while true; do
            renameFile=$(hdfs dfs -ls  $folder'/*/*' 2>&1 | grep 'Attempting to complete')
            if [[ ! -z $renameFile ]]; then
                filePath=$(sed 's/\(.* file \)\(.*\) during\(.*\)/\2/' <<< $renameFile)
                procFilePath=${filePath%/*}
                echo "Found file /$procFilePath-RenamePending.json"
                hdfs dfs -rm "/$procFilePath-RenamePending.json"
            else
                echo "No files remaining in this folder"
                break
            fi
        done
    done
    echo "All hbase folders clean and no rename pending files ..."
}
echo "This is a diagnostics tool that will check your HDI HBase cluster for issues"
echo "First verifying Health of HMaster process"
if ismasterrunning; then 
    echo "HMaster running fine; now moving to check regions in transition"; 
else
    echo "Looking for hbase folders with pending rename files that might be causing HMaster start failures..."  
    cleanuppendingrenamefiles
    retryCount=0
    while [[ $retryCount -le 10 ]]; do
        if ismasterrunning; then
            echo "HBase is now up and running"
            break
        else
            echo "Could not start HBase Master, retrying in some time"
        fi
        ((retryCount++))
        sleep 60
    done
    if [[ $retryCount -eq 11 ]]; then
        echo "HBase Master not starting up; contact support - hbase@microsoft.com"
        exit
    fi
fi

