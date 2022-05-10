#! /bin/bash
#----------------------------------------------------------------------------------
# THIS SCRIPT STOPS HBASE, CHANGES CONFIGS, CLEANS ZK, RESTORES WAL, COPIES APPS AND RESTARTS SERVICES
# THIS SCRIPT IS TO BE EXECUTED ON THE DESTINATION CLUSTER IN THE MIGRATION.
# THE SCRIPT MUST BE EXECUTED FROM EITHER OF THE ZOOKEEPER HOSTS.

# TODO: Replace tabs with spaces. Will unnecessarily mark each and every line changed and difficult to review. To be done in a separate patch.

#----------------------------------------------------------------------------------
# PRINT USAGE INFORMATION
print_usage() {
    cat << ...
	Usage:
	sudo bash migrate-hbase-dest.sh  -f <src_default_Fs>

	Mandatory arguments:
	--------------------

	-f, --src-fs
		Root of def fs src cluster
		For example:
		-f wasb://anynamehbase0316encoder-2021-03-17t01-07-55-935z@anynamehbase0hdistorage.blob.core.windows.net
...
exit 132
}

#----------------------------------------------------------------------------------
# INITIALIZE PARAMETERS
AMBARI_USER=hdinsightwatchdog
AMBARI_PASSWORD=
CLUSTER=
HOST_FQDN=
DEFAULTFS=
SRC_CLUSTER_AW=false
SRC_CLUSTER_HDI_VER=
DST_HBASE_VER=
TMP_FILE_DIR="./tmp"
ACTIVEAMBARIHOST=
ESP_CLUSTER=
RETRY_INTERVAL=3
WU_RETRY_COUNT=65 # Total wait time = WU_RETRY_COUNT * RETRY_INTERVAL seconds; from Ambari config HBase RegionServer shutdown timeout is 180
STALE_CONFIG_RESTARTS=401 # Total wait time = STALE_CONFIG_RESTARTS * RETRY_INTERVAL seconds; Ambari Agent logs show {'timeout': 20} minutes.
AMBARI_CONFIG_SCRIPT="/var/lib/ambari-server/resources/scripts/configs.py"

#------------------------------------------------------------------------------------
# PARSE AND PROCESS COMMAND LINE ARGUMENTS
process_arguments()
{
	while :; do
		case $1 in
				-h|--help)
						print_usage
						exit
						;;
				-f|--src-fs)
						if [ -n "$2" ]; then
								DEFAULTFS=$2
								shift
						else
								printf '[ERROR] -f or --src-fs requires non-empty reference to default FS of src cluster.' >&2
								print_usage
								exit 1
						fi
						;;
				--src-fs=?*)
						DEFAULTFS=${1#*=}
						;;
				--src-fs=)
						# Handle the case where no argument is specified after '=' sign
						printf '[ERROR] -f or --src-fs requires non-empty reference to default FS of src cluster.' >&2
						print_usage
						exit 1
						;;

				-?*)
						printf '[WARN] Ignoring unknown option: %s\n' "$1" >&2
						;;

				*)
			   	# Breaking out of while loop as there are no more arguments left.
					break
		esac

		shift
	done

	if [ -z "$DEFAULTFS" ]
	then
		printf '[ERROR] -f or --src-fs is mandatory.' >&2
		print_usage
		exit 1
	fi
	echo "Root of def fs src cluster: $DEFAULTFS"
}

setupClusterManifestParser() {
	echo "Initiating cluster manifest parser"
	python -m pip install -U watchdog
}

#------------------------------------------------------------------------------------
# GET THE ACTIVE AMBARI HOST, MAKE SURE THE SCRIPTS ARE EXECUTED ON A ZK HOST AND GET THE CLUSTER NAME
getPrimaryHNCheckZKHostAndSetClusterName() {
# getPrimaryHNCheckZKHostAndSetClusterName() {
	# Get the Active Ambari Host.
    ACTIVEAMBARIHOST=$(grep headnodehost /etc/hosts | cut -f6)
	if [[ $ACTIVEAMBARIHOST == "headnodehost." ]] || [[ $ACTIVEAMBARIHOST == *"cloudapp.net." ]]; then
        ACTIVEAMBARIHOST=$(grep headnodehost /etc/hosts | cut -f5)
    fi
    echo "primary headnode=$ACTIVEAMBARIHOST. Lower case: ${ACTIVEAMBARIHOST,,}"
	if [[ -z $ACTIVEAMBARIHOST ]]; then
        echo "Could not determine active Ambari host. Exiting."
	    exit 139
    fi

	# Get Host Name and make sure it is a ZK host.
    HOST_FQDN=$(hostname -f)
	echo "Host=$HOST_FQDN"
	if [[ -z "HOST_FQDN" ]] || [[ $HOST_FQDN != zk* ]]; then
		echo "The script must be executed on a ZooKeeper host. Exiting."
		exit 139
	fi

	# Get the cluster name.
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
		echo "kinit with principal: $PRINCIPAL"
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
		echo "Error: The script has to be run as root. Exiting."
		print_usage
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
# CHANGE DEFAULT FS.
changeDefaultFS()
{
	echo "Now changing defaultfs: $DEFAULTFS"
	$AMBARI_CONFIG_SCRIPT -a set -n "$CLUSTER" -l $ACTIVEAMBARIHOST -c core-site -k fs.defaultFS -v "$DEFAULTFS" -p "$AMBARI_PASSWORD" -u "$AMBARI_USER"
	if [ $? -ne 0 ]; then
		echo "Error: Failed changing fs.defaultFS. Exiting."
		exit 1
	fi

	echo "Done changing defaultfs: $DEFAULTFS"
}

#----------------------------------------------------------------
# GET HBASE WAL DIR AND VERSION.
get_hbase_wal_dir_and_version()
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
	HBASEVERSION=$(echo $(hbase version) | sed -n "s/^.*HBase \s*\(\S*\).*$/\1/p")
	echo "DEBUG: VERSIONTAG=$VERSIONTAG"
	echo "DEBUG: TEMPHBASEWALDIR=$TEMPHBASEWALDIR"
	echo "HBASEWALDIR=$HBASEWALDIR"
	echo "HBASEVERSION=$HBASEVERSION"
	echo ""
}

#----------------------------------------------------------------
# UPDATE HBASE ROOT DIR.
update_hbase_root_dir() {
	echo "Updating hbase.rootdir to $DEFAULTFS/hbase"
	$AMBARI_CONFIG_SCRIPT -a set -n "$CLUSTER" -l $ACTIVEAMBARIHOST -c hbase-site -k hbase.rootdir -v "$DEFAULTFS/hbase" -p "$AMBARI_PASSWORD" -u "$AMBARI_USER"
	if [ $? -ne 0 ]; then
		echo "Error: Failed to update hbase.rootdir. Exiting."
		exit 1
	fi

	echo "Done updating hbase.rootdir."
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
			echo "Warning: Error executing curl to obtain service state. Retrying after $RETRY_INTERVAL seconds."
			sleep $RETRY_INTERVAL
			continue
		fi
		cluster_state=$(grep state $TMP_FILE_DIR/wait_until.ot | awk '{print $3}' | sed -e 's/"//g' | sed -e 's/,.*//g' | tail -1)
		if [[ $cluster_state == "$1" ]]; then
			echo "Current State: $cluster_state. Found state as $1 for $2."
			break
      	fi

		echo "Current State: $cluster_state. Retrying after $RETRY_INTERVAL seconds."
      	sleep $RETRY_INTERVAL
	done

	if (( i>$WU_RETRY_COUNT )); then
		echo "ERROR: Failed waiting for $2 to reach the state $1."
		cat $TMP_FILE_DIR/wait_until.ot
		cat $TMP_FILE_DIR/wait_until.err
		exit 1
	fi
}

#----------------------------------------------------------------
# CREATE CONFIG
create_config()
{
	echo "Create config:"
	echo '{"type": "'"$1"'", "tag": "install", "properties": '"$2"'}'
    curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -X POST -d '{"type": "'"$1"'", "tag": "install", "properties": '"$2"'}' https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/configurations -H "X-Requested-By:ambari" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/create_config.err
	if [ $? -ne 0 ]; then
		echo "Error: Error executing curl to create config. Exiting."
		exit 1
	fi
}

#----------------------------------------------------------------
# ADD CONFIG
apply_config()
{
	echo "Apply config: "
	echo '{"Clusters": {"desired_configs": { "type": "'"$1"'", "tag" :"install" }}}'
    curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -X PUT -d '{"Clusters": {"desired_configs": { "type": "'"$1"'", "tag" :"install" }}}' https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER -H "X-Requested-By:ambari" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/apply_config.err
	if [ $? -ne 0 ]; then
		echo "Error: Error executing curl to apply config. Exiting."
		exit 1
	fi
}

#----------------------------------------------------------------
# REMOVE HBASE ZNODE
remove_hbase_znode()
{
	echo "Removing hbase znode"
	if [[ $ESP_CLUSTER == "Y" ]]; then
hbase zkcli << ... > $TMP_FILE_DIR/hbase.out 2>&1
rmr /hbase-secure
quit
...
	else
hbase zkcli << ... > $TMP_FILE_DIR/hbase.out 2>&1
rmr /hbase-unsecure
quit
...
	fi

	echo "Done removing hbase znode"
}

#----------------------------------------------------------------
# GET INFORMATION ABOUT THE OLD CLUSTER
init_old_cluster_migrate_info()
{
	hdfs dfs -copyToLocal "$DEFAULTFS/src-migrate-info.txt" $TMP_FILE_DIR
	if [ $? -ne 0 ]; then
		echo "Error: Error copying old cluster info. Exiting."
		exit 1
	fi

	echo "Old cluster config: "
	cat $TMP_FILE_DIR/src-migrate-info.txt
	echo ""
	SRC_CLUSTER_HDI_VER=$(grep HDI $TMP_FILE_DIR/src-migrate-info.txt | awk '{ print $2}')
	if grep -q AW $TMP_FILE_DIR/src-migrate-info.txt; then
		SRC_CLUSTER_AW=true
	fi

	echo "Source cluster version is $SRC_CLUSTER_HDI_VER; Source cluster accelrated writes (true/empty[false]): $AW"
}

#----------------------------------------------------------------
# USE THIS METHOD TO STOP A SERVICE.
# stop_service <Service Name> <URL>
stop_service() {
	SERVICE_NAME=$1
	URL=$2

	echo "Stopping $SERVICE_NAME."
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -X GET https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services/HBASE -H "X-Requested-By:ambari" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/hbase.err
	if [ $? -ne 0 ]; then
    	echo "Warning: Failed executing curl to obtain state of $SERVICE_NAME.";
	else
		SERVICE_STATE=$(grep "\"state\"" $TMP_FILE_DIR/hbase.json | awk '{ print $3 }' | sed -e 's/"//g')
		echo "State of $SERVICE_NAME: $SERVICE_STATE"
	fi


	if [[ $SERVICE_STATE != "INSTALLED" ]]; then
		echo "Stopping $SERVICE_NAME."

		curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"'"Stopping $SERVICE_NAME"'"}, "HostRoles": {"state": "INSTALLED"}}' "$URL" -o $TMP_FILE_DIR/curl.out 2> $TMP_FILE_DIR/curl.err
		if [ $? -ne 0 ]; then
			echo "Error: Failed executing curl to stop service $SERVICE_NAME. Exiting."
			exit 1
		fi

		grep -q "Accepted" $TMP_FILE_DIR/curl.out # This will fail if accepted is not found because we are running with -e
		if [ $? -ne 0 ]; then
			echo "Error: The curl command to stop $SERVICE_NAME was not accepted. Exiting."
			exit 1
		fi

		wait_until "INSTALLED" $SERVICE_NAME
	fi
}

#----------------------------------------------------------------
# USE THIS METHOD TO START A SERVICE.
# start_service <Service Name> <URL>
start_service() {
	SERVICE_NAME=$1
	URL=$2
	echo "Starting $SERVICE_NAME."
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"'"Starting $SERVICE_NAME"'"}, "HostRoles": {"state": "STARTED"}}' "$URL" -o $TMP_FILE_DIR/curl.out 2> $TMP_FILE_DIR/curl.err
	if [ $? -ne 0 ]; then
		echo "Error: Failed executing curl to start service $SERVICE_NAME. Exiting."
		exit 1
	fi

	grep -q "Accepted\|HTTP.* 200 " $TMP_FILE_DIR/curl.out # This will fail if accepted is not found because we are running with -e
	if [ $? -ne 0 ]; then
		echo "Error: The curl command to start $SERVICE_NAME was not accepted. Exiting."
		exit 1
	fi

	wait_until "STARTED" $SERVICE_NAME
}

#----------------------------------------------------------------
# CREATE PYTHON FILE FOR GENERATING JSON FILE TO RESTART SERVICES
py_for_restart_services_json() {
	cat > ./tmp/generate_restart_services_json.py << ...
import json
#import pprint
import hdinsight_common.ClusterManifestParser as ClusterManifestParser

data=None
with open('./tmp/stale_services_trimmed.json') as f:
    data=json.load(f)
# pp = pprint.PrettyPrinter(depth=6)
ol={}
ol["level"]="CLUSTER"
ol["cluster_name"]=ClusterManifestParser.parse_local_manifest().deployment.cluster_name
ri={}
ri["command"]="RESTART"
ri["context"]="RestartHostComponents"
ri["operation_level"]=ol
ri["command_retry_enabled"]=True
#pp.pprint(ri)
restartJson={}
restartJson["RequestInfo"]=ri
# pp.pprint(restartJson)

rf=[]
for k in data:
    for v1 in data[k]:
        for k2 in v1:
            if k2 != "HostRoles":
                continue
            d2 = v1[k2]
            od1={}
            for k3 in d2:
                if k3 == "host_name":
                    od1["hosts"] = d2[k3]
                elif k3 == "service_name" or k3 == "component_name":
                    od1[k3]=d2[k3]
            rf.append(od1)

restartJson["Requests/resource_filters"]=rf
#pp.pprint(restartJson)
#print "========================================="
#print restartJson
json_object = json.dumps(restartJson,separators=(',', ':'))
with open ('./tmp/restart_services.json', 'w') as f:
    f.write (json_object)
#print tvar
...
}

#----------------------------------------------------------------
# poll_on_request <request id>
poll_on_request() {
	req_id=$1
	echo "Polling on request id $req_id."
	for (( i=1 ; i<=$STALE_CONFIG_RESTARTS ; i++ )); do
		curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -X GET https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/requests/$req_id\?fields=Requests -H "X-Requested-By:ambari" -o $TMP_FILE_DIR/restart_poll.out 2> $TMP_FILE_DIR/restart_poll.err
		cur_req_state=$(grep "\"request_status\"" $TMP_FILE_DIR/restart_poll.out | awk '{print $3}' | awk -NF \" '{print $2}')
		if [[ $cur_req_state == "COMPLETED" ]]; then
			echo "Request state: $cur_req_state. Request $req_id COMPLETED."
			break
		elif [[ $cur_req_state == "FAILED" ]]; then
			echo "Request state: $cur_req_state. Request $req_id FAILED."
			echo "ERROR: Failed waiting for $req_id to reach COMPLETED state. PLEASE MANUALLY ISSUE OF A RESTART FROM AMBARI AS REQUESTED BY THE AMBARI UI. Exiting."
			exit 1
		fi

		echo "Request state: $cur_req_state. Retrying after $RETRY_INTERVAL seconds."
		sleep $RETRY_INTERVAL
	done
	if (( i>$STALE_CONFIG_RESTARTS )); then
		echo "ERROR: Failed waiting for $req_id to reach COMPLETED state. PLEASE MANUALLY ISSUE OF A RESTART FROM AMBARI AS REQUESTED BY THE AMBARI UI. Exiting. "
		cat $TMP_FILE_DIR/wait_until.ot
		cat $TMP_FILE_DIR/wait_until.err
		exit 1
	fi
}

#----------------------------------------------------------------
# RESTART STALE CONFIGS
restart_stale_configs()
{
	echo "Restarting stale config services"
	# curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -X GET https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/host_components\?HostRoles/stale_configs=true\&fields=HostRoles/service_name,HostRoles/host_name\&minimal_response=false -H "X-Requested-By:ambari" -o $TMP_FILE_DIR/stale_services_json 2> $TMP_FILE_DIR/restart_stale_configs.err

	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -H "X-Requested-By:ambari" -X GET https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/host_components\?HostRoles/stale_configs=true\&fields=HostRoles/service_name,HostRoles/host_name\&minimal_response=true -o $TMP_FILE_DIR/stale_services_json 2> $TMP_FILE_DIR/restart_stale_configs.err
	if [ $? -ne 0 ]; then
		echo "Error: Error fetching stale configs. Exiting"
		exit 1
	fi

	sleep $RETRY_INTERVAL

	fline=$(grep -h -n -m 1 "^\{$" 	$TMP_FILE_DIR/stale_services_json | awk -NF ':' '{print $1}')
	eline=$(grep -h -n "^\}" 		$TMP_FILE_DIR/stale_services_json | tail -1 | awk -NF ':' '{print $1}')
	sed -n "${fline},${eline}p" 	$TMP_FILE_DIR/stale_services_json > ./tmp/stale_services_trimmed.json # ./tmp/stale_services_trimmed.json is used in py_for_restart_services_json

	py_for_restart_services_json # Generates a python file to parse ./tmp/stale_services_trimmed.json and generate a json file ./tmp/restart_services.json for restart services command.

	python ./tmp/generate_restart_services_json.py # Generates a json file called ./tmp/restart_services.json which contains services to be restarted in json format as required by Ambari.

	restart_issued="Y" # Only intialization as yes.
	restart_services_var=$(cat ./tmp/restart_services.json)
	echo "Services to be restarted: $restart_services_var"

	# echo $restart_services_var | grep "\"Requests/resource_filters\":\[\]"
	# if [ $? -ne 0 ]; then
	if [[ $restart_services_var == *"\"Requests/resource_filters\":[]"*  ]] ; then
		echo "There are no stale services to be restarted."
		restart_issued="N"
	else
		curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -X POST -d $restart_services_var https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/requests -H "X-Requested-By:ambari" -o $TMP_FILE_DIR/restart_request.out 2> $TMP_FILE_DIR/restart_request.err

		if [ $? -ne 0 ]; then
			# echo ""; echo "Warning: requesting restart of stale configs failed. PLEASE MANUALLY ISSUE OF A RESTART FROM AMBARI AS REQUESTED BY THE AMBARI UI."; echo ""
			# restart_issued="N"
			echo "Error: requesting restart of stale configs failed. PLEASE MANUALLY ISSUE OF A RESTART FROM AMBARI AS REQUESTED BY THE AMBARI UI. Exiting."
			exit 1
		fi

		grep -q "Accepted" $TMP_FILE_DIR/restart_request.out # This will fail if accepted is not found because we are running with -e
		if [ $? -ne 0 ] ; then
			# echo ""; echo "Warning: The curl command to restart stale config services was not accepted. PLEASE MANUALLY ISSUE OF A RESTART FROM AMBARI AS REQUESTED BY THE AMBARI UI."; echo ""
			# cat $TMP_FILE_DIR/restart_request.out
			# cat $TMP_FILE_DIR/restart_request.err
			# restart_issued="N"
			echo "Error: The curl command to restart stale config services was not accepted. PLEASE MANUALLY ISSUE OF A RESTART FROM AMBARI AS REQUESTED BY THE AMBARI UI. Exiting."
			exit 1
		fi
	fi


	if [[ $restart_issued == "Y" ]]; then
		echo "The restart of stale configs was issued."

		req_id=$(grep "\"id\""  $TMP_FILE_DIR/restart_request.out | awk '{print $3}' | awk -NF ',' '{print $1}')
		echo "Restart request ID: $req_id"

		# Poll on the req_id
		poll_on_request $req_id
	fi

	# grep host_components $TMP_FILE_DIR/stale_services_json | grep -v stale |rev | cut -d'"' -f2 | rev > $TMP_FILE_DIR/list_of_components
	# TODO: Simplify it -- we restart all except HBase and PHOENIX here.
	# egrep 'RANGER|DATANODE|HDFS|NAMENODE' $TMP_FILE_DIR/list_of_components > $TMP_FILE_DIR/list_of_components_final || true # force successs because not any of these components might be present in the list.
	# egrep -v 'RANGER|DATANODE|HDFS|NAMENODE|HBASE|PHOENIX' $TMP_FILE_DIR/list_of_components >> $TMP_FILE_DIR/list_of_components_final || true # similar reasoning as above.
	# egrep -v 'HBASE|PHOENIX' $TMP_FILE_DIR/list_of_components > $TMP_FILE_DIR/list_of_components_final || true # similar reasoning as above.
	# echo "Restarting below stale configs: "
	# cat $TMP_FILE_DIR/list_of_components_final
	# echo ""
	# for URL in `cat $TMP_FILE_DIR/list_of_components_final`
	# do
	# 	SERVICE_NAME=`echo $URL|rev|cut -d'/' -f1|rev`

	# 	stop_service 	$SERVICE_NAME $URL
	# 	start_service 	$SERVICE_NAME $URL
	# done
	echo "Done restarting services with stale configs."
}

#----------------------------------------------------------------
# STOP HBase
stop_hbase()
{
	echo "Stopping HBase"
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -X GET https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services/HBASE -H "X-Requested-By:ambari" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/hbase.err
	if [ $? -ne 0 ]; then
		echo "Warning: Error executing curl to get HBase state."
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

		grep -q "Accepted" $TMP_FILE_DIR/hbase.json # This will fail if accepted is not found because we are running with -e
		if [ $? -ne 0 ]; then
			echo "Error: The curl command to stop HBase was not accepted. Exiting."
			exit 1
		fi

		wait_until "INSTALLED" "HBASE"
	fi

	echo "Stopped HBase"
}

#----------------------------------------------------------------
# START HBase
start_hbase()
{
	echo "Starting HBase"
	curl -u $AMBARI_USER:$AMBARI_PASSWORD -i -X PUT -d '{"RequestInfo" : {"context" : "Start HBASE via REST"}, "Body" : {"ServiceInfo":{"state":"STARTED"}}}' https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services/HBASE -H "X-Requested-By:ambari" -o $TMP_FILE_DIR/hbase.json 2> $TMP_FILE_DIR/hbase.err
	if [ $? -ne 0 ]; then
		echo "Warning: Failed executing curl to start HBase."
		# exit 1
	fi

	grep -q "Accepted" $TMP_FILE_DIR/hbase.json # This will fail if accepted is not found because we are running with -e
	if [ $? -ne 0 ]; then
		echo ""
		echo "Warning: The curl command to start HBase was not accepted. PLEASE START HBASE FROM AMBARI UI"
		echo ""
		#exit 1
	else
		wait_until "STARTED" "HBASE"
	fi
}

#----------------------------------------------------------------
# HDFS DFS CP commands
hdfs_cp()
{
	# Tracing execution of cp command because it is a long running command.
	echo "Executing cp from $1 to $2"
	# sudo -u hbase hdfs dfs -cp "$DEFAULTFS/hbase-wal-backup/hbasewal/*" "$DEFAULTFS/hbase-wals"
	hdfs dfs -cp -f $1 $2
	if [ $? -ne 0 ]; then
		echo "Error: Copy command failed. Exiting."
		exit 1
	fi

	echo "Done copying from $1 to $2"
}

hdfs_cp_D()
{
	# Tracing execution of cp command because it is a long running command.
	echo "Executing cp from $1 to $2"
	# sudo -u hbase hdfs dfs -cp "$DEFAULTFS/hbase-wal-backup/hbasewal/*" "$DEFAULTFS/hbase-wals"
	#hdfs dfs -Dfs.azure.page.blob.dir="/hbase/WALs,/hbase/MasterProcWALs,/hbase/oldWALs" -cp $1 $2
	hdfs dfs -Dfs.azure.page.blob.dir="/hbase/WALs,/hbase/MasterProcWALs" -cp -f $1 $2
	if [ $? -ne 0 ]; then
		echo "Error: Copy command failed. Exiting."
		exit 1
	fi

	echo "Done copying from $1 to $2"
}


#----------------------------------------------------------------
# DELETE DIR. Usage: delete_dir <directory to be deleted> <ignore_failure>
delete_dir() {
	echo "Deleting $1."
	hdfs dfs -rm -r $1
	if [ $? -ne 0 ]; then
		if [[ $2 == "ignore_failure" ]] ; then
			echo ""; echo "Warning: Failed to delete dir: $1."; echo ""
		else
			echo "Error: Failed to delete $1. Exiting."
			exit 1
		fi

	fi
}

#----------------------------------------------------------------
# CREATE DIR. Usage: create_dir <directory to be created>  <ignore_failure>
create_dir() {
	echo "Creating dir:$1"
	hdfs dfs -mkdir -p "$1"
	if [ $? -ne 0 ]; then
		if [[ $2 == "ignore_failure" ]] ; then
			echo ""; echo "Warning: Failed to create dir: $1."; echo ""
		else
			echo "Error: Failed to create dir: $1. Exiting."
			exit 1
		fi

	fi
}

#----------------------------------------------------------------
# COPY WALs.
do_WAL_copy() {
	echo "Starting WAL copy"
	if [[ $HBASEWALDIR == "hdfs://mycluster/hbasewal" ]]; then
		echo "The destination cluster is an AW cluster."
		update_hbase_root_dir
		delete_dir "hdfs://mycluster/hbasewal/MasterProcWALs"	"ignore_failure"
		delete_dir "hdfs://mycluster/hbasewal/WALs"				"ignore_failure"
		# oldWALs, recoverd_edits, corrupt and data are not required. data is for recovered edits only.
		if [[ $SRC_CLUSTER_AW == true ]]; then
			echo "The source cluster is also an AW cluster. Both src and dest are AW."
			hdfs_cp "$DEFAULTFS/hbase-wal-backup/*" 	"hdfs://mycluster/hbasewal"
			# Above shall copy MasterProcWALs and WALs dirs
			# hdfs_cp "$DEFAULTFS/hbase-wal-backup/WALs" 			"hdfs://mycluster/hbasewal"
		else
			# Direct copy, no use of the backup folder.
			echo "The source cluster is not an AW cluster."
			if [[ $SRC_CLUSTER_HDI_VER == 3.6 ]]; then
				echo "The source cluster is HDI 3.6."
				hdfs_cp_D "$DEFAULTFS/hbase/MasterProcWALs" 	"hdfs://mycluster/hbasewal"
				hdfs_cp_D "$DEFAULTFS/hbase/WALs" 				"hdfs://mycluster/hbasewal"
			else
				echo "The source cluster is not HDI 3.6."
				hdfs_cp	"$DEFAULTFS/hbase-wals/MasterProcWALs" 	"hdfs://mycluster/hbasewal"
				hdfs_cp	"$DEFAULTFS/hbase-wals/WALs" 		   	"hdfs://mycluster/hbasewal"
			fi
		fi
	else
		echo "The destination cluster is not an AW cluster."
		if [[ $SRC_CLUSTER_AW == true ]]; then
			echo "The source cluster is an AW cluster."
			if [[ $HBASEVERSION == 2* ]]; then
				echo "Destination cluster is HBase 2*; HDI 4.0"
				delete_dir 	"$DEFAULTFS/hbase-wals/MasterProcWALs"	"ignore_failure"
				delete_dir 	"$DEFAULTFS/hbase-wals/WALs"			"ignore_failure"
				create_dir 	"$DEFAULTFS/hbase-wals"					"ignore_failure"
				hdfs_cp 	"$DEFAULTFS/hbase-wal-backup/*" "$DEFAULTFS/hbase-wals"
			else
				# HDI 3.6 cluster
				echo "Destination cluster is HBase 1*; HDI 3.6"
				delete_dir 	"$DEFAULTFS/hbase/MasterProcWALs"	"ignore_failure"
				delete_dir 	"$DEFAULTFS/hbase/WALs"				"ignore_failure"
				hdfs_cp 	"$DEFAULTFS/hbase-wal-backup/*" "$DEFAULTFS/hbase"
			fi
		else
			echo "The source cluster is not an AW cluster. Both src and dest are non AW."
			if [[ $HBASEVERSION == 2* ]]; then
				echo "Destination cluster is HBase 2*; HDI 4.0"
				if [[ $SRC_CLUSTER_HDI_VER == 3.6 ]]; then
					echo "The source cluster is HBase 1*; HDI 3.6. Dest is HBase 2*; HDI 4.0"
					delete_dir 	"$DEFAULTFS/hbase-wals/MasterProcWALs"	"ignore_failure"
					delete_dir 	"$DEFAULTFS/hbase-wals/WALs"			"ignore_failure"
					create_dir 	"$DEFAULTFS/hbase-wals"					"ignore_failure"
					hdfs_cp_D "$DEFAULTFS/hbase/MasterProcWALs" 	"$DEFAULTFS/hbase-wals/"
					hdfs_cp_D "$DEFAULTFS/hbase/WALs" 				"$DEFAULTFS/hbase-wals/"
				else
					echo "Both destination and source clusters are HBase 2*; HDI 4.0 and both are non-AW. No copy required."
				fi
			else
				echo "Destination cluster is HBase 1*; HDI 3.6"
				if [[ $SRC_CLUSTER_HDI_VER == 4.0 ]]; then
					echo "The source cluster is HBase 2*; HDI 4.0. The Dest cluster is HBase 1*; HDI 3.6."
					delete_dir 	"$DEFAULTFS/hbase/MasterProcWALs"	"ignore_failure"
					delete_dir 	"$DEFAULTFS/hbase/WALs"				"ignore_failure"
					hdfs_cp	"$DEFAULTFS/hbase-wals/MasterProcWALs" 	"$DEFAULTFS/hbase/"
					hdfs_cp	"$DEFAULTFS/hbase-wals/WALs" 			"$DEFAULTFS/hbase/"
				else
					echo "Both destination and source clusters are HBase 1*; HDI 3.6 and both are non-AW. No copy required."
				fi
			fi
		fi
	fi
	echo "Done WAL copy"
}

#----------------------------------------------------------------
# COPY BINARIES.
copy_binaries() {
	echo "Copying binaries"
	dest_apps_dir=
	src_apps_dir=
	if [[ $ESP_CLUSTER == "Y" ]]; then
		if [[ $SRC_CLUSTER_HDI_VER == 3.6 ]] ; then
			if [[ $HBASEVERSION == 2* ]]; then
				echo "A migration from HDI 3.6 to HDI 4.0 cluster"
				dest_apps_dir="/hdinsight/apps"
				# create the path like destinations in the source cluster container.
				create_dir 	"$DEFAULTFS$dest_apps_dir"	"ignore_failure"
				# Specify the src_apps_dir var.
				src_apps_dir="/hdinsight/apps"
			else
				echo "Migration from HDI 3.6 to HDI 3.6"
				echo "Please copy /hdp/apps or /apps directory, as it exists from container of the new cluster to the old clusters container (yes, new container to old) AND ISSUE RESTARTS AS REQUESTED IN THE AMBARI UI."
				echo "The old cluster's container's path was provided as argument to this scricpt after -f."
				echo "The new cluster's container's path is not required to be explicity specified as the commands are being executed from the new cluster only."
				echo "Please note that for ESP clusters kinit must be done for the hhbasedfs service user to execute the above command."
				echo "Example:"
				echo "sudo bash"
				echo "klist -kt /etc/security/keytabs/hbase.service.keytab"
				echo "kinit -kt /etc/security/keytabs/hbase.service.keytab <PRICIPAL as exctracted from the above command>"
				echo "hdfs dfs -cp /hdp/apps/<hdi-version> <source-container-fullpath>/hdp/apps # Here /hdp/apps is for illustration, choose between /hdp/apps and /apps based on the one that exists."
				echo "Exiting."
				exit 0
				# src_apps_dir="/hdp/apps"
				# dest_apps_dir="/hdp/apps"
			fi
		else
			echo "Migrating from HDI 4.0. The Destination must be HDI 4.0"
			dest_apps_dir="/hdinsight/apps"
			src_apps_dir="/hdinsight/apps"
		fi
	else
		dest_apps_dir="/hdp/apps"
		src_apps_dir="/hdp/apps"
	fi
	echo "Contents of the Destiation dir: $DEFAULTFS$dest_apps_dir. "
	hdfs dfs -ls $DEFAULTFS$dest_apps_dir
	echo "Contents of the Source dir: $src_apps_dir. "
	hdfs dfs -ls $src_apps_dir
	hdfs dfs -cp "$src_apps_dir/*" "$DEFAULTFS$dest_apps_dir/"
	if [ $? -ne 0 ]; then
		echo ""; echo "Warning: Copy from $src_apps_dir to $DEFAULTFS$dest_apps_dir/ failed"; echo ""
	else
		echo "Done copying binaries"
	fi
}

#----------------------------------------------------------------
# MAIN
#----------------------------------------------------------------
echo "This script stops HBase, changes configs, cleans ZK, restores WAL, copies apps and restarts stale services because of config changes."
process_arguments $@
root_user_check
del_and_create_tmp_dir
get_ambari_user_n_pass
getPrimaryHNCheckZKHostAndSetClusterName
#validate_ambari_credentials
esp_cluster_init
init_old_cluster_migrate_info
stop_hbase
changeDefaultFS
remove_hbase_znode
get_hbase_wal_dir_and_version
do_WAL_copy
copy_binaries
restart_stale_configs
start_hbase
echo "Done executing the script."
echo "PLEASE RESTART THE ACTIVE HMASTER FROM THE AMBARI UI IF DEAD REGION SERVERS ARE SHOWN IN THE ACTIVE HMASTER UI."
echo ""