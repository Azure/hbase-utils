#! /bin/bash

usage() {
    echo ""
    echo "Usage: sudo -E bash HBaseRecoveryScript.sh [storage-account-name] [number of worker nodes]" ;
    echo "This script does NOT require Ambari username and password";
    exit 132;
}

validateUsernameAndPassword() {
	echo "Validating the username and password"
    coreSiteContent=$($AMBARICONFIGS_PY --user=$USERID --password=$PASSWD --action=get --port=$PORT --host=$ACTIVEAMBARIHOST --cluster=$CLUSTERNAME --config-type=core-site)
	
    if [[ $coreSiteContent == *"[ERROR]"* && $coreSiteContent == *"Bad credentials"* ]]; then
        echo "[ERROR] Username and password are invalid. Exiting!"
        exit 134
    fi
}

updateAmbariConfigs() {
    updateResult=$($AMBARICONFIGS_PY --user=$USERID --password=$PASSWD --action=set --port=$PORT --host=$ACTIVEAMBARIHOST --cluster=$CLUSTERNAME --config-type=core-site -k "fs.defaultFS" -v "$STORAGEACCOUNT")
    
    if [[ $updateResult != *"Tag:version"* ]] && [[ $updateResult == *"[ERROR]"* ]]; then
        echo "[ERROR] Failed to update core-site. Exiting!"
        echo $updateResult
        exit 135
    fi
    echo "Added property: 'fs.defaultFS':$STORAGEACCOUNT"

    updateResult=$($AMBARICONFIGS_PY --user=$USERID --password=$PASSWD --action=set --port=$PORT --host=$ACTIVEAMBARIHOST --cluster=$CLUSTERNAME --config-type=hbase-site -k "hbase.rootdir" -v "$STORAGEACCOUNT/hbase")
	if [[ $updateResult != *"Tag:version"* ]] && [[ $updateResult == *"[ERROR]"* ]]; then
		echo "[ERROR] Failed to update hbase-site. Exiting!"
		echo $updateResult
		exit 135
	fi
	echo "Added property: 'hbase.rootdir':$STORAGEACCOUNT/hbase "
}

function restart_stale_services() {
    sudo python << EOF
from hdinsight_common.AmbariHelper import AmbariHelper
ambari_helper = AmbariHelper()
ambari_helper.restart_all_stale_services()
EOF
}

remove_zk_data()
{
  echo "Removing the parent hbase zk node"
  cd /usr/hdp/current/zookeeper-client/bin
  echo 'rmr /hbase-unsecure' | sudo ./zkCli.sh -server $HOSTNAME:2181 -n
  status_code=$?
  if [ ${status_code} -ne 0 ]; then
    echo "The command may have failed."
  fi
}

delete_current_wal_directory() 
{
  echo "Deleting the WAL directories from hdfs"
  if ! sudo -u hbase hdfs dfs -rm -r hdfs://mycluster/hbasewal; then
		echo "Error deleting hdfs://mycluster/hbasewal"
		exit 1
  fi
}

stop_hbase()
{
  echo "Stopping HBASE service"
  stop_command=$(curl -u $USERID:$PASSWD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Stop HBase via REST"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://$ACTIVEAMBARIHOST:$PORT/api/v1/clusters/$CLUSTERNAME/services/HBASE)
  if [ `echo $stop_command | grep -c "Accepted" ` -gt 0 ]
  then
    echo "REST Command executed successfully"
  else
    echo "REST Command execution failed, check the curl command. Stop the HBase services manually from AMBARI and then call refresh_hbase function. Make sure metatable.txt file is present, if not create the file with 'echo 'scan "hbase:meta", { COLUMNS => ["info:sn"] }' | hbase shell -n > metatable.txt' command"
    exit 1
  fi
}

# WAIT UNTIL   
wait_until()
{
	TMP_FILE_DIR="/tmp"
	echo "Waiting for $2 to reach the state $1."
	for (( i=1 ; i<=$3 ; i++ ));
	do
		# Capture the state
		curl -u $USERID:$PASSWD https://$CLUSTERNAME.azurehdinsight.net/api/v1/clusters/$CLUSTERNAME/services/$2?fields=ServiceInfo/state -o $TMP_FILE_DIR/wait_until.ot 2> $TMP_FILE_DIR/wait_until.err
		if [ $? -ne 0 ]; then
			echo "Warning: Error executing curl to obtain service state."
		fi

		cluster_state=$(grep state $TMP_FILE_DIR/wait_until.ot | awk '{print $3}' | sed -e 's/"//g' | sed -e 's/,.*//g' | tail -1)
		if [[ $cluster_state == "$1" ]]; then
			echo "State: $cluster_state. Found state as $1 for $2."
			break
      	fi

		echo "State: $cluster_state. Retrying after 30 seconds."
      	sleep 30
	done

	if (( i>6 ));	then
		echo "ERROR: Timedout waiting for $2 to reach the state $1."
		cat $TMP_FILE_DIR/wait_until.ot
		cat $TMP_FILE_DIR/wait_until.err
		exit 1
	fi
}

refresh_hbase()
{

  regionservers=$(cat metatable.txt | grep wn | awk '{print $4}' | sort | uniq) 

  #Delete the current WALs from hdfs and create empty directories with old cluster Regionservers
  delete_current_wal_directory

  for i in $regionservers
  do
    i=${i:6}
    sudo -u hbase hdfs dfs -mkdir -p hdfs://mycluster/hbasewal/WALs/$i;
  done

  #remove the zk data
  remove_zk_data

  #Start HBase Services from AMBARI
  echo "Starting HBASE"
  startResult=$(curl -u $USERID:$PASSWD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Start HBase via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://$ACTIVEAMBARIHOST:$PORT/api/v1/clusters/$CLUSTERNAME/services/HBASE)
  if [[ $startResult == *"500 Server Error"* || $startResult == *"internal system exception occurred"* ]]; then
        sleep 60
        echo "Retrying starting HBASE"
		startResult=$(curl -u $USERID:$PASSWD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Start HBase via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://$ACTIVEAMBARIHOST:$PORT/api/v1/clusters/$CLUSTERNAME/services/HBASE)
  fi
  echo $startResult
}

#Get the HOSTNAME of one of the zookeeper nodes
HOSTNAME=$(cat /etc/hosts | grep zk | cut -d ' ' -f3 | sort | uniq | head -n1)
echo $HOSTNAME

#Import helper module
wget -O /tmp/HDInsightUtilities-v01.sh -q https://hdiconfigactions.blob.core.windows.net/linuxconfigactionmodulev01/HDInsightUtilities-v01.sh && source /tmp/HDInsightUtilities-v01.sh && rm -f /tmp/HDInsightUtilities-v01.sh

STORAGEACCOUNT=$1
NUMBEROFNODES=$2
REFRESHTIMEOUT=NUMBEROFNODES*10
echo "Storage Account : $STORAGEACCOUNT"
AMBARICONFIGS_PY=/var/lib/ambari-server/resources/scripts/configs.py
PORT=8080

CLUSTERNAME=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nprint ClusterManifestParser.parse_local_manifest().deployment.cluster_name" | python)

USERID=$(echo -e "import hdinsight_common.Constants as Constants\nprint Constants.AMBARI_WATCHDOG_USERNAME" | python)

echo "USERID=$USERID"

PASSWD=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nimport hdinsight_common.Constants as Constants\nimport base64\nbase64pwd = ClusterManifestParser.parse_local_manifest().ambari_users.usersmap[Constants.AMBARI_WATCHDOG_USERNAME].password\nprint base64.b64decode(base64pwd)" | python)

ACTIVEAMBARIHOST=headnodehost

validateUsernameAndPassword

#Update the storage account configurations 
echo "***************************UPDATING AMBARI CONFIG**************************"
updateAmbariConfigs
echo "***************************UPDATED AMBARI CONFIG**************************"

#stop hbase services
stop_hbase

wait_until "INSTALLED" "HBASE" "NUMBEROFNODES"

#remove the zk data
remove_zk_data 

#Delete the current WALs from hdfs and create empty directories with old cluster Regionservers
delete_current_wal_directory

#restart all the services with stale configurations from Ambari
echo "Restarting services with stale configs"
restart_stale_services

echo "Waiting for services to restart"
wait_until "STARTED" "HBASE" "NUMBEROFNODES"
sleep 3m

while true 
do
  sudo echo 'scan "hbase:meta", { COLUMNS => ["info:sn"] }' | hbase shell -n > metatable.txt
  if ! grep -q ERROR metatable.txt; then
    break
  fi
done

#stop the HBase services from Ambari
stop_hbase
wait_until "INSTALLED" "HBASE" "REFRESHTIMEOUT"

refresh_hbase
