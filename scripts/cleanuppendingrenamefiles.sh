#/bin//bash
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
echo "All hbase folders clean and no rename pending files remain; master should start now; checking status..."
retryCount=0
while [[ $retryCount -le 5 ]]; do
    hbaseStatus=$(echo "status"|hbase shell 2>&1 | grep "1 active master")
    if [[ ! -z $hbaseStatus ]]; then
        echo "HBase is now up and running"
        break
    else
        echo "Could not start HBase Master, retrying in some time"
    fi
    ((retryCount++))
    sleep 60
done
if [[ $retryCount -eq 6 ]]; then
    echo "HBase Master not starting up; contact support - hdihbase@microsoft.com"
fi

