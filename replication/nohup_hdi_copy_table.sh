#!/bin/bash 

DEFAULT_LOG_FILE=/var/log/hbase/copyTable.out

DIR_NAME=`dirname $DEFAULT_LOG_FILE`

if [[ ! -d "$DIR_NAME" ]];
then
	mkdir $DIR_NAME
	RESULT=$?
	if [[ $RESULT -ne 0 ]];
	then
		echo "[ERROR] Failed to create directory '$DIR_NAME'."
	fi
fi
echo "Here"
wget https://raw.githubusercontent.com/Azure/hbase-utils/gkanade-fixrepforcustomnamespace/replication/hdi_copy_table.sh -O /tmp/hdi_copy_table.sh
echo "Here"
chmod +x /tmp/hdi_copy_table.sh

echo "[INFO] Starting copy table operation that will take a while. The progress will be saved in $DEFAULT_LOG_FILE."
echo "nohup bash /tmp/hdi_copy_table.sh $@"
nohup bash /tmp/hdi_copy_table.sh $@  > $DEFAULT_LOG_FILE  &

