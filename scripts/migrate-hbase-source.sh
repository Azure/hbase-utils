#! /bin/bash
#----------------------------------------------------------------------------------
# USAGE:
# THIS SCRIPT STOPS HBASE AND MAKES BACKUP OF WAL IF NEEDED.
# THIS SCRIPT IS TO BE EXECUTED ON THE SOURCE CLUSTER IN THE MIGRATION.
# THE SCRIPT MUST BE EXECUTED FROM EITHER OF THE ZOOKEEPER HOSTS.
#----------------------------------------------------------------------------------

#----------------------------------------------------------------------------------
# INITIALIZE PARAMETERS
#----------------------------------------------------------------------------------
AMBARI_USER=hdinsightwatchdog
AMBARI_PASSWORD=
CLUSTER=
HOST_FQDN=
TMP_FILE_DIR="./tmp"
ACTIVEAMBARIHOST=
RETRY_INTERVAL=3 # seconds.
ESP_CLUSTER=
WU_RETRY_COUNT=62 # HBase restart timeout = WU_RETRY_COUNT * RETRY_INTERVAL; from Ambari config HBase RegionServer shutdown timeout is 180

# TODO: Replace tabs with spaces. Will unnecessarily mark each and every line changed and difficult to review. To be done in a separate patch.

#----------------------------------------------------------------------------------
# PRINT USAGE INFORMATION
print_usage() {
	echo "Usage: sudo bash migrate-hbase-source.sh";
	echo "Note: the script must be executed from either of the ZooKeeper hosts."
	exit 132; # Exit code 132 means illegal instruction vis-a-vis incorrect usage.
}

setupClusterManifestParser() {
	echo "Initiating cluster manifest parser"
	python -m pip install -U watchdog
}

#----------------------------------------------------------------
# ENSURE THAT THE HOST IS A ZK HOST AND GET THE CLUSTER NAME.
getPrimaryHNCheckZKHostAndSetClusterName() {
	# Commented out the below as these vars are not really used in this script.
    # PRIMARYHEADNODE=`get_primary_headnode`
    # echo "primary headnode=$PRIMARYHEADNODE. Lower case: ${PRIMARYHEADNODE,,}"
    # # Check if values retrieved are empty, if yes, exit with error
    # if [[ -z $PRIMARYHEADNODE ]]; then
    #     echo "Could not determine primary headnode. Exiting."
	#     exit 139
    # fi
    # ACTIVEAMBARIHOST=$PRIMARYHEADNODE

	# Get Host Name and make sure it is a ZK host.
    HOST_FQDN=$(hostname -f)
	echo "Host=$HOST_FQDN"
	if [[ -z "HOST_FQDN" ]] || [[ $HOST_FQDN != zk* ]]; then
		echo "The script must be executed on a ZooKeeper host. Exiting."
		exit 139
	fi

	setupClusterManifestParser

    # CLUSTER=$(sed -n -e 's/.*\.\(.*\)-ssh.*/\1/p' <<< HOST_FQDN)
    # echo "Cluster Name=$CLUSTER"
    # if [ -z "$CLUSTER" ]; then
    #     CLUSTER=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nprint ClusterManifestParser.parse_local_manifest().deployment.cluster_name" | python)
    #     if [ $? -ne 0 ]; then
    #         echo "[ERROR] Cannot determine cluster name. Exiting!"
    #         exit 133
    #     fi
    # fi
	CLUSTER=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nprint ClusterManifestParser.parse_local_manifest().deployment.cluster_name" | python)
	if [ $? -ne 0 ]; then
		echo "[ERROR] Cannot determine cluster name. Exiting!"
		exit 133
	fi

    echo "Cluster Name=$CLUSTER"
}

#----------------------------------------------------------------
# INIT FOR ESP CLUSTERS
esp_cluster_init() {
	# Get if the cluster is ESP or not.
	# TODO: Get it from cluster manifest if it exists.
	echo "Checking if cluster is ESP or standard."
    curl -u $AMBARI_USER:$AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER?fields=Clusters/security_type" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/hbase.err
	if [ $? -ne 0 ]; then
    	echo "Error executing curl to obtain cluster security type. Exiting."
		exit 1
	fi

    TEMP_ESP_TYPE=$(grep security_type $TMP_FILE_DIR/hbase.json | awk '{print $3}' | sed -e 's/"//g' | sed -e 's/,.*//g' | tail -1)

	# Set ESP flag and do kinit if the cluster is an ESP cluster.
    if [[ $TEMP_ESP_TYPE == "KERBEROS" ]]; then
		echo "The cluster is an ESP cluster."
        ESP_CLUSTER=Y
        THIS_HOST=$(hostname)
        PRINCIPAL=$(klist -kt /etc/security/keytabs/hbase.service.keytab | grep hbase | tail -1 | awk '{print $4}')
		echo "kinit with PRINCIPAL: $PRINCIPAL"
        kinit -kt /etc/security/keytabs/hbase.service.keytab $PRINCIPAL
		if [ $? -ne 0 ]; then
			echo "Error executing kinit. Exiting."
			exit 1
		fi

	else
		echo "The cluster is a standard (non-ESP) cluster."
    fi
}

#----------------------------------------------------------------
# WAIT UNTIL <STATE> <SERVICE>
wait_until()
{
	echo "Waiting for $2 to reach the state $1."
	for (( i=1 ; i<=$WU_RETRY_COUNT ; i++ ));
	do
		# Capture the state
		curl -u $AMBARI_USER:$AMBARI_PASSWORD https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services/$2?fields=ServiceInfo/state -o $TMP_FILE_DIR/wait_until.ot 2> $TMP_FILE_DIR/wait_until.err
		if [ $? -ne 0 ]; then
			echo "Warning: Error executing curl to obtain service state."
		fi

		cluster_state=$(grep state $TMP_FILE_DIR/wait_until.ot | awk '{print $3}' | sed -e 's/"//g' | sed -e 's/,.*//g' | tail -1)
		# echo "State: $cluster_state"
		if [[ $cluster_state == "$1" ]]; then
			echo "State: $cluster_state. Found state as $1 for $2."
			break
      	fi

		echo "State: $cluster_state. Retrying after $RETRY_INTERVAL seconds."
      	sleep $RETRY_INTERVAL
	done

	if (( i>$WU_RETRY_COUNT ));	then
		echo "ERROR: Timedout waiting for $2 to reach the state $1."
		cat $TMP_FILE_DIR/wait_until.ot
		cat $TMP_FILE_DIR/wait_until.err
		exit 1
	fi
}

#----------------------------------------------------------------
# STOP HBase
stop_hbase()
{
	# echo "STOPPING HBASE"
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -X GET https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services/HBASE -H "X-Requested-By:ambari" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/hbase.err
	if [ $? -ne 0 ]; then
    	echo "Warning: Failed executing curl to obtain HBase cluster state."
	fi

	HBASESTATE=$(grep "\"state\"" $TMP_FILE_DIR/hbase.json | awk '{ print $3 }' | sed -e 's/"//g')
	echo "State of HBase: $HBASESTATE"

	if [[ $HBASESTATE != "INSTALLED" ]]; then
		echo "Stopping HBase"
		curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -X PUT -d '{"RequestInfo" : {"context" : "Stop HBASE via REST"}, "Body" : {"ServiceInfo":{"state":"INSTALLED"}}}' https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services/HBASE -H "X-Requested-By:ambari" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/hbase.err
		if [ $? -ne 0 ]; then
			echo "Error: Failed executing curl to stop HBase. Exiting."
			exit 1
		fi

		grep -q "Accepted" $TMP_FILE_DIR/hbase.json # This will fail if accepted is not found when we are running with set -e
		if [ $? -ne 0 ]; then
			echo "Error: The curl command to stop HBase was not accepted. Exiting."
			exit 1
		fi

		wait_until "INSTALLED" "HBASE"
	fi

	echo "Stopped HBase"
}

#----------------------------------------------------------------
# DELETE AND CREATE TEMPORARY DIRECTORY FOR LOGS ETC.
del_and_create_tmp_dir()
{
	echo "Deleting and recreating $TMP_FILE_DIR."
	if ! rm -rf $TMP_FILE_DIR || ! mkdir $TMP_FILE_DIR; then
		echo "Error deleting or creating $TMP_FILE_DIR. Exiting."
		exit 1
	fi
}

#----------------------------------------------------------------
# ENSURE THE SCRIPT IS EXECUTED WITH SUDO.
root_user_check()
{
	if [ "$(id -u)" != "0" ]; then
		echo "Error: The script has to be run as root."
		print_usage
	fi
}

#----------------------------------------------------------------------------------
# IMPORT A HELPER MODULE
import_helper_module()
{
	wget -O $TMP_FILE_DIR/HDInsightUtilities-v01.sh -q https://hdiconfigactions.blob.core.windows.net/linuxconfigactionmodulev01/HDInsightUtilities-v01.sh && source $TMP_FILE_DIR/HDInsightUtilities-v01.sh && rm -f $TMP_FILE_DIR/HDInsightUtilities-v01.sh
	if [ $? -ne 0 ]; then
		echo "Error: Failed importing helper module. Exiting."
		exit 1
	fi
}

#----------------------------------------------------------------
# GET AMBARI USER NAME AND PASSWORD.
get_ambari_user_n_pass()
{
	AMBARI_USER=$(echo -e "import hdinsight_common.Constants as Constants\nprint Constants.AMBARI_WATCHDOG_USERNAME" | python)
	echo "Ambari USERID=$AMBARI_USER"
	AMBARI_PASSWORD=$(echo -e "import hdinsight_common.ClusterManifestParser as ClusterManifestParser\nimport hdinsight_common.Constants as Constants\nimport base64\nbase64pwd = ClusterManifestParser.parse_local_manifest().ambari_users.usersmap[Constants.AMBARI_WATCHDOG_USERNAME].password\nprint base64.b64decode(base64pwd)" | python)
}

#----------------------------------------------------------------
# GET HBASE WAL DIR.
get_hbase_wal_dir()
{
	# TODO: Use a different method to get the latest config, if that is the case.
	echo "Getting HBase WAL dir."
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER?fields=Clusters/desired_configs/hbase-site" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/hbase.err
	if [ $? -ne 0 ]; then
		echo "Error: Failed executing curl to get latest version tag. Exiting."
		exit 1
	fi

	if ! grep -q tag $TMP_FILE_DIR/hbase.json ; then # This will fail if accepted is not found because we are running with -e
		echo "Error: 'tag' not found in output of hbase-site desired configs. Exiting."
		exit 1
	fi

	VERSIONTAG=$(grep tag $TMP_FILE_DIR/hbase.json  | awk '{ print $3 }' | sed -e 's/"//g' | sed -e 's/,.*//g')
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/configurations?type=hbase-site&tag=$VERSIONTAG" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/hbase.err
	if [ $? -ne 0 ]; then
		echo "Error: Failed executing curl to get latest hbase-site. Exiting."
		exit 1
	fi

	TEMPHBASEWALDIR=$(cat $TMP_FILE_DIR/hbase.json | grep "hbase.wal.dir" | awk '{ print $3 }')
	# NORMALIZE PARAMETERS
	HBASEWALDIR=$(echo ${TEMPHBASEWALDIR} | sed -e 's/"//g' | sed -e 's/,$//g')
	echo "DEBUG: VERSIONTAG=$VERSIONTAG"
	echo "DEBUG: TEMPHBASEWALDIR=$TEMPHBASEWALDIR"
	echo "HBASEWALDIR=$HBASEWALDIR"
	echo ""
}

#----------------------------------------------------------------
# EXECUTE AN HDFS CP COMMAND.
# USAGE: hdfs_cp <source> <destination>
hdfs_cp()
{
	# Tracing execution of cp command because it is a long running command.
	echo "Executing cp from $1 to $2"
	# sudo -u hbase hdfs dfs -cp "$DEFAULTFS/hbase-wal-backup/hbasewal/*" "$DEFAULTFS/hbase-wals"
	hdfs dfs -cp $1 $2
	# hdfs dfs -copyToLocal $1 $2
	if [ $? -ne 0 ]; then
		echo "Error: Copy operation failed. Exiting."
		exit 1
	fi

	echo "Done copying from $1 to $2"
	## For ref:
	# sshuser@hn0-kulw4a:~$ sudo hdfs dfs -copyToLocal  hdfs://mycluster/hbasewal/MasterProcWALs /hbase-wal-backup
	# sshuser@hn0-kulw4a:~$ ls /hbase-wal-backup
	# MasterProcWALs
	# sshuser@hn0-kulw4a:~$
}

#----------------------------------------------------------------
# COPY REQUIRED HBASE WALS
do_hbase_wal_copy()
{
	echo "Do HBase WAL backup."
	if [[ $HBASEWALDIR == "hdfs://mycluster/hbasewal" ]]; then
		echo "Deleting /hbase-wal-backup"
		hdfs dfs -rm -r /hbase-wal-backup # || true # A cleanup operation before execution.
		if [ $? -ne 0 ]; then
			echo "Warning: Failed deleting /hbase-wal-backup."
		fi

		echo "Creating /hbase-wal-backup"
		hdfs dfs -mkdir /hbase-wal-backup
		if [ $? -ne 0 ]; then
			echo "Error: Failed creating WAL backup directory. Exiting."
			exit 1
		fi

		# echo "Executing cp from hdfs://mycluster/hbasewal to /hbase-wal-backup."
		# hdfs dfs -cp hdfs://mycluster/hbasewal /hbase-wal-backup
		hdfs_cp hdfs://mycluster/hbasewal/MasterProcWALs /hbase-wal-backup
		hdfs_cp hdfs://mycluster/hbasewal/WALs			 /hbase-wal-backup
		# Copy only WALs and Master Proc WALs
		# Use hadoop dist cp -- not really required because MasterProcWALs and WALs should be small.
		# Pass multiple source dirs if possible, it will prevent multiple process inits. Not possible with copyToLocal
	else
		echo "HBase WAL backup copy is not required for non Accelrated write clusters. Therefore, not doing WAL backup."
	fi
}

#----------------------------------------------------------------
# Store cluster info in src-migrate-info.txt
store_cluster_info()
{
	touch $TMP_FILE_DIR/src-migrate-info.txt
	if [ $? -ne 0 ]; then
		echo "Error: Failed touch $TMP_FILE_DIR/src-migrate-info.txt. Exiting."
		exit 1
	fi

	HBASEVERSION=$(echo $(hbase version) | sed -n "s/^.*HBase \s*\(\S*\).*$/\1/p")
	echo "HBase version: $HBASEVERSION"
	if [[ $HBASEVERSION == 2* ]]; then
		echo "HDI 4.0" >> $TMP_FILE_DIR/src-migrate-info.txt
	else
		echo "HDI 3.6" >> $TMP_FILE_DIR/src-migrate-info.txt
	fi

	# The below information we have already.
	# curl -u $AMBARI_USER:$AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/configurations?type=hbase-site&tag=$VERSIONTAG" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/hbase.err
	# TEMPHBASEWALDIR=`cat $TMP_FILE_DIR/hbase.json | grep "hbase.wal.dir" | awk '{ print $3 }'`
	# HBASEWALDIR=`echo ${TEMPHBASEWALDIR} | sed -e 's/"//g' | sed -e 's/,$//g'`
	if [[ $HBASEWALDIR == "hdfs://mycluster/hbasewal" ]]; then
		echo "AW" >> $TMP_FILE_DIR/src-migrate-info.txt
	fi

	echo "src-migrate-info.txt contents:"
	cat $TMP_FILE_DIR/src-migrate-info.txt
	echo ""

	hdfs dfs -copyFromLocal -f $TMP_FILE_DIR/src-migrate-info.txt / # It overwites the existing file.
	if [ $? -ne 0 ]; then
		echo "Error: Failed copying $TMP_FILE_DIR/src-migrate-info.txt. Exiting."
		exit 1
	fi
}

#----------------------------------------------------------------
# MAIN
#----------------------------------------------------------------
##############################
echo "Begin script. This script stops HBase and takes WAL backup if required."
root_user_check
del_and_create_tmp_dir
import_helper_module
get_ambari_user_n_pass
getPrimaryHNCheckZKHostAndSetClusterName
#validate_ambari_credentials
esp_cluster_init
stop_hbase
get_hbase_wal_dir
do_hbase_wal_copy
store_cluster_info
echo "Successfully executed the script. Done."
