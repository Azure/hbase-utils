#! /bin/bash

#------------------------------------------------------------------
# THIS SCRIPT ENABLES HBASE REPLICATION BETWEEN TWO HBASE CLUSTERS 
#------------------------------------------------------------------


#----------------------------------------------------------------
# PRINT USAGE INFORMATION
#----------------------------------------------------------------


print_usage()
{
cat << ...
Usage: 
$0 -s <src_cluster_dns> -sp <src_ambari_password> -d <dst_cluster_dns> -dp <dst_ambari_password> [optional arguments]

Mandatory arguments:
--------------------

-s, --src-cluster              
                                DNS name of the source HBase cluster.
                                For example: 
                                -s hbsrccluster
                                --src-cluster=hbsrccluster

-d, --dst-cluster               
                                DNS name of the destination (replica) HBase cluster.
                                For example: 
                                -s dsthbcluster
                                --src-cluster=dsthbcluster

-sp, --src-ambari-password      
                                Admin password for Ambari of source HBase cluster.

-dp, --dst-ambari-password      
                                Admin password for Ambari of destination HBase cluster.

Optinal arguments:
------------------

-su, --src-ambari-user          
                                Admin username for Ambari of source HBase cluster.
                                Default = admin.

-du, --dst-ambari-user          
                                Admin username for Ambari of destination HBase cluster.
                                Default = admin.

-t, --table-list                
                                ';' separated list of tables to be replicated. 
                                
                                For example: --table-list="table1;table2;table3"
                                By default - all hbase tables are replicated.

-m, --machine                   
                                This option should be used when running the $0 script as 
                                Script Action from HDInsight portal or Azure Powershell.
                                the value of -m should be either hn0 or hn1.

-ip								
                                This argument acts as a switch to utilize the static IP's of zookeeper
                                nodes from replica cluster instead of FQDN names. The static IP's 
                                needs to be pre-configured before enabling replication. 
                                This argument is mandatory when enabling replication across two 
                                different VNET's.

-cp, -copydata
                                This option is a switch which enables the migration of 
                                existing data on the tables where replication gets enabled.

-rpm, -replicate-phoenix-meta
                                This switch enables the replication on Phoenix system (SYSTEM.*)
                                tables. 

                                NOTE: This option needs to be used with caution!
                                It is in general advised to recreate phoenix tables on replica
                                cluster before using this script.

-x, -suffix
                                This option enables to specify custom cluster suffix; 
                                default is azurehdinsight.net
-h, --help                      
                                Display's usage information.

Sample Commands:
------------------

1) To enable replication on all tables without migrating existing data:

   $0 -s pri-hbcluster -d sec-hbcluster -sp Mypassword\!789 -dp Mypassword1234#
 
2) To enable replication on tables specified (table1, table2 and table3) and also migrating the existing data, use following command:

   $0 --src-cluster=pri-hbcluster --dst-cluster=sec-hbcluster --src-ambari-user=admin --src-ambari-password=Hello\!789 --dst-ambari-user=admin --dst-ambari-password=Sample1234# --table-list="table1;table2;table3" -cp

...
exit 132
}

#------------------------------------------------------------------
# INITIALIZE PARAMETERS
#------------------------------------------------------------------

SRC_CLUSTER=
SRC_AMBARI_USER=admin
SRC_AMBARI_PASSWORD=
DST_CLUSTER=
DST_AMBARI_USER=admin
DST_AMBARI_PASSWORD=
TABLE_LIST=
AMBARICONFIGS_SH=/var/lib/ambari-server/resources/scripts/configs.sh
PORT=8080
MACHINE=
USE_IP=false
MIGRATE_EXISTING_DATA=false
REPLICATE_PHOENIX_SYSTEM_TABLES=false
SUFFIX=azurehdinsight.net

#------------------------------------------------------------------
# PARSE AND PROCESS COMMAND LINE ARGUMENTS
#------------------------------------------------------------------

process_arguments()
{
	while :; do
		case $1 in
			-h|--help)  
				print_usage
				exit
				;;

			-s|--src-cluster)  
				if [ -n "$2" ]; then
					SRC_CLUSTER=$2
					shift
				else
					printf '[ERROR] -s or --src-cluster requires non-empty DNS name of source HBase cluster.' >&2
					print_usage
					exit 1
				fi
				;;

			--src-cluster=?*)
				SRC_CLUSTER=${1#*=} 
				;;

			--src-cluster=)
			# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -s or --src-cluster requires non-empty DNS name of source HBase cluster.' >&2
				print_usage
				exit 1
				;;

			-d|--dst-cluster)  
				if [ -n "$2" ]; then
					DST_CLUSTER=$2
					shift
				else
					printf '[ERROR] -d or --dst-cluster requires non-empty DNS name of destination (replica) HBase cluster.' >&2
					print_usage
					exit 1
				fi
				;;

			--dst-cluster=?*)
				DST_CLUSTER=${1#*=} 
				;;

			--dst-cluster=)
			# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -d or --dst-cluster requires non-empty DNS name of destination (replica) HBase cluster.' >&2
				print_usage
				exit 1
				;;

			-su|--src-ambari-user)  
				if [ -n "$2" ]; then
					SRC_AMBARI_USER=$2
					shift
				else
					printf '[ERROR] -su or --src-ambari-user requires non-empty ambari admin user name.' >&2
					print_usage
					exit 1
				fi
				;;

			--src-ambari-user=?*)
				SRC_AMBARI_USER=${1#*=} 
				;;
			--src-ambari-user=)
			# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -su or --src-ambari-user requires non-empty ambari admin user name.' >&2
				print_usage
				exit 1
				;;

			-sp|--src-ambari-password)  
				if [ -n "$2" ] 
				then
					SRC_AMBARI_PASSWORD=$2
					shift
				else
					printf '[ERROR] -sp or --src-ambari-password requires non-empty ambari admin user password.' >&2
					print_usage
					exit 1
				fi
				;;

			--src-ambari-password=?*)
				SRC_AMBARI_PASSWORD=${1#*=} 
				;;

			--src-ambari-password=)
			# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -sp or --src-ambari-password requires non-empty ambari admin user password.' >&2
				print_usage
				exit 1
				;;

			-du|--dst-ambari-user)  
				if [ -n "$2" ] 
				then
					DST_AMBARI_USER=$2
					shift
				else
					printf '[ERROR] -du or --dst-ambari-user requires non-empty ambari admin user name.' >&2
					print_usage
					exit 1
				fi
				;;

			--dst-ambari-user=?*)
				DST_AMBARI_USER=${1#*=} 
				;;

			--dst-ambari-user=)
				# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -du or --dst-ambari-user requires non-empty ambari admin user name.' >&2
				print_usage
				exit 1
				;;

			-dp|--dst-ambari-password)  
				if [ -n "$2" ] 
				then
					DST_AMBARI_PASSWORD=$2
					shift
				else
					printf '[ERROR] -sp or --dst-ambari-password requires non-empty ambari admin user password.' >&2
					print_usage
					exit 1
				fi
				;;

			--dst-ambari-password=?*)
				DST_AMBARI_PASSWORD=${1#*=} 
				;;

			--dst-ambari-password=)
				# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -dp or --dst-ambari-password requires non-empty ambari admin user password.' >&2
				print_usage
				exit 1
				;;

			-x|--suffix)
				if [ -n "$2" ]
				then
				        SUFFIX=$2
					shift
				else
				        printf '[ERROR] -x or --suffix requires non-empty suffix.' >&2
					print_usage
					exit 1
				fi
				;;

			--suffix=?*)
				SUFFIX=${1#*=}
				;;

                        --suffix=)
				# Handle the case where no argument is specificed after '=' sign.
				printf '[ERROR] -x or --suffix requires non-empty suffix.' >&2
				print_usage
				exit 1
				;;

			-t|--table-list)  
				if [ -n "$2" ] 
				then
					TABLE_LIST=$2
					shift
				else
					printf '[ERROR] -t or --table-list requires non-empty list of tables to be replicated.' >&2
					print_usage
					exit 1
				fi
				;;

			--table-list=?*)
				TABLE_LIST=${1#*=} 
				;;

			--table-list=)
				# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -t or --table-list requires non-empty list of tables to be replicated.' >&2
				print_usage
				exit 1
				;;

			-m|--machine)  
				if [ -n "$2" ]; 
				then
					MACHINE=$2
					shift
				else
					printf '[ERROR] -t or --table-list requires non-empty list of tables to be replicated.' >&2
					print_usage
					exit 1
				fi
				;;

			--machine=?*)
				MACHINE=${1#*=} 
				;;

			--machine=)
				# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -m or --machine requires non-empty machine name.' >&2
				print_usage
				exit 1
				;;

			-ip)
				USE_IP=true
				;;

			-cp)
				MIGRATE_EXISTING_DATA=true
				;;

			-copydata)
				MIGRATE_EXISTING_DATA=true
				;;

			-rpm)
				REPLICATE_PHOENIX_SYSTEM_TABLES=true
				;;

			-replicate-phoenix-meta)
				REPLICATE_PHOENIX_SYSTEM_TABLES=true
				;;

			--)
				shift
				break
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

	# NORMALIZE PASSWORDS
	#
	echo $SRC_AMBARI_PASSWORD | sed -e 's/\\//g' > /tmp/passwd.txt
	SRC_AMBARI_PASSWORD=$(cat /tmp/passwd.txt)

	echo $DST_AMBARI_PASSWORD | sed -e 's/\\//g' > /tmp/passwd.txt
	DST_AMBARI_PASSWORD=$(cat /tmp/passwd.txt)

}

#------------------------------------------------------------------
# VALIDATE MANDATORY COMMAND LINE PARAMETERS
#------------------------------------------------------------------

validate_arguments()
{
	if [[ -z "${SRC_CLUSTER// }" ]] || [[ -z "${SRC_AMBARI_PASSWORD// }" ]] || [[ -z "${DST_CLUSTER// }" ]] || [[ -z "${DST_AMBARI_PASSWORD// }" ]] 
	then
		printf '[ERROR] Mandatory arguments missing.\n' >&2
		print_usage
		exit 1
	fi

	if [[ ! -z $MACHINE ]] && [[ $MACHINE != hn* ]] 
	then
		printf '[ERROR] -m accepts only hn0 or hn1 as arguments.\n' >&2
		exit 1
	fi

	# MACHINE VALIDATION.
	#
	THIS_MACHINE=`hostname`

	if [[ $THIS_MACHINE != $MACHINE* ]] 
	then
		printf '[ERROR] Not the correct machine to execute the script. Exiting!\n' >&2
		exit 0
	fi
}

#------------------------------------------------------------------
# VALIDATE AMBARI CREDENTIALS
#------------------------------------------------------------------

validate_ambari_credentials() 
{
	SRC_AMBARI_PASSWORD=`echo $SRC_AMBARI_PASSWORD`

	curl -u $SRC_AMBARI_USER:$SRC_AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$SRC_CLUSTER.$SUFFIX/api/v1/clusters/$SRC_CLUSTER?fields=Clusters/desired_configs/hbase-site" -o /tmp/hbase.json 2> /dev/null
	grep -i "access.*denied" /tmp/hbase.json > /dev/null 2>&1

	RESULT=$?
	if [ $RESULT -eq 0 ]
	then
		echo "[ERROR] Invalid Ambari username or password for cluster $SRC_CLUSTER. Exiting!"
		cat /tmp/hbase.json | sed -e 's/^/[INFO] /g'
		exit 134
	else
		echo "[INFO] Primary cluster credentials successfully validated."
	fi

	curl -u $DST_AMBARI_USER:$DST_AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$DST_CLUSTER.$SUFFIX/api/v1/clusters/$DST_CLUSTER?fields=Clusters/desired_configs/hbase-site" -o /tmp/hbase.json 2> /dev/null
	grep -i "access.*denied" /tmp/hbase.json > /dev/null 2>&1

	RESULT=$?
	if [ $RESULT -eq 0 ] 
	then
		echo "[ERROR] Invalid Ambari username or password for cluster $DST_CLUSTER Exiting!"
		cat /tmp/hbase.json | sed -e 's/^/[INFO] /g'
		exit 134
	else
		echo "[INFO] Destination cluster credentials successfully validated."
	fi
}

#------------------------------------------------------------------
# GET DESTINATION REPLICATION PEER
#------------------------------------------------------------------

set_replication_peer () 
{
	curl -u $DST_AMBARI_USER:$DST_AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$DST_CLUSTER.$SUFFIX/api/v1/clusters/$DST_CLUSTER?fields=Clusters/desired_configs/hbase-site" -o /tmp/hbase.json 2> /dev/null
	grep tag /tmp/hbase.json > /dev/null

	if (( $? !=  0 ))  
	then
		echo "[ERROR] Could not set replication peer."
		echo "[ERROR] Curl command failed to query '$DST_CLUSTER' due to following reason:"
		cat /tmp/hbase.json  | sed -e 's/^/[INFO] /g'
		exit 1
	fi

	local VERSIONTAG=`grep tag /tmp/hbase.json  | awk '{ print $3 }' | sed -e 's/"//g' | sed -e 's/,.*//g'`
	curl -u $DST_AMBARI_USER:$DST_AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$DST_CLUSTER.$SUFFIX/api/v1/clusters/$DST_CLUSTER/configurations?type=hbase-site&tag=$VERSIONTAG" -o /tmp/hbase.json 2> /dev/null

	local TEMPQUORUM=`cat /tmp/hbase.json | grep "hbase.zookeeper.quorum" | awk '{ print $3 }'`
	local TEMPPORT=`cat /tmp/hbase.json | grep "hbase.zookeeper.property.clientPort" | awk '{ print $3 }'`
	local TEMPPARENT=`cat /tmp/hbase.json | grep "zookeeper.znode.parent" | awk '{ print $3 }'`

	# NORMALIZE PARAMETERS
	#
	local ZKQUORUM=`echo ${TEMPQUORUM} | sed -e 's/"//g' | sed -e 's/,$//g'`
	local ZKPORT=`echo ${TEMPPORT} | sed -e 's/"//g' | sed -e 's/,$//g'`
	local ZKPARENT=`echo ${TEMPPARENT} | sed -e 's/"//g' | sed -e 's/,$//g'`

	if [[ $USE_IP == true ]]
	then

		TEMP_IFS=$IFS
		ZKQUORUMIP=""

		IFS=',' read -ra ZK_MACHINE_ARRAY <<< "$ZKQUORUM"
		for ZK_MACHINE in "${ZK_MACHINE_ARRAY[@]}"
		do
			curl -u $DST_AMBARI_USER:$DST_AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$DST_CLUSTER.$SUFFIX/api/v1/clusters/$DST_CLUSTER/hosts/$ZK_MACHINE" -o /tmp/hbase.json 2> /dev/null
			ZKIP=`grep \"ip\" /tmp/hbase.json | awk '{ print $3 }' | sed -e 's/"//g' | sed -e 's/,$//g'`

			if [[ ! -z $ZKQUORUMIP ]]
			then
				ZKQUORUMIP="$ZKIP,$ZKQUORUMIP"
			else
				ZKQUORUMIP=$ZKIP
			fi 
		done
		IFS=$TEMP_IFS

		REPLICATION_PEER=$ZKQUORUMIP:$ZKPORT:$ZKPARENT
	else
		REPLICATION_PEER=$ZKQUORUM:$ZKPORT:$ZKPARENT
	fi

	echo "[INFO] Identified sink cluster peer address as: $REPLICATION_PEER."

	hbase shell << ... 2> /dev/null | sed -e 's/^/[INFO] /g'
	remove_peer '1'
	list_peers
	add_peer '1', CLUSTER_KEY => "$REPLICATION_PEER"
	list_peers
	exit
...
}

#------------------------------------------------------------------
# SET ARRAY OF TABLES TO REPLICATE
#------------------------------------------------------------------

set_tables_to_replicate ()
{
	# IF USER PROVIDES A LIST OF TABLES TO REPLICATE, THEN REPLICATE THOSE.
	# OTHERWISE, REPLICATE ALL TABLES.
	#
	if [[ ! -z $TABLE_LIST ]]
	then
		TABLES_ARRAY=(`echo $TABLE_LIST | sed -e 's/;/ /g'`)
	else
                # Retrieve a list of tables for replication using hbase shell command
                hbase shell <<< "list"> tables.out
		TABLES_ARRAY=($(awk '/TABLE/{f=1; next}; /row\(s\)/{f=0} f' tables.out))
	fi

	# TODO: VALIDATION OF TABLES IS NOT EASY AS LIST OPERATION COULD TAKE TIME. 
	# FOR CUSTOMER'S WHO HAVE 1000'S OF TABLES, PROVIDING COMMAND LINE ARGUMENT 
	# IS BETTER FOR REPLICATION.
}

#------------------------------------------------------------------
#  MAIN
#------------------------------------------------------------------

process_arguments $@

validate_arguments

validate_ambari_credentials

set_replication_peer

TABLES_ARRAY=()
set_tables_to_replicate 

# DECLARING A MAP OF TABLES AND THE TIMESTAMPS AT WHICH REPLICATION 
# GOT ENABLED ON THEM.
#
declare -A TABLE_TS_MAP

#------------------------------------------------------------------
#  ENABLE REPLICATION ON ALL DESIRED TABLES
#------------------------------------------------------------------

for user_table in "${TABLES_ARRAY[@]}"
do

	if [[ $user_table == SYSTEM* ]] && [[ $REPLICATE_PHOENIX_SYSTEM_TABLES == false ]]
	then
		echo "[INFO] Ignoring the replication for phoenix system table '$user_table'."
		continue;
	fi

	echo "[INFO] Attempting to enable replication for table '$user_table'."
	echo "[INFO] Extracting schema for '$user_table' from HBase cluster '$SRC_CLUSTER'."
	echo "[INFO] Applying schema of '$user_table' to HBase cluster '$DST_CLUSTER'."

	# CREATE TABLE IN DEST CLUSTER AND ENABLE REPLICATION
	#
	hbase shell << ... > /tmp/hbase.out 2>&1
	enable_table_replication '$user_table'
	describe '$user_table'
	exit
...

	CUR_TIMESTAMP=$(($(date +%s%N)/1000000))

	# CHECK FOR ERRORS.
	#
	grep "ERROR:" /tmp/hbase.out > /dev/null 2>&1 

	RESULT_VAL=$?

	# VALIDATE WHETHER REPLICATION WAS ENABLED SUCCESSFULLY OR NOT.
	# INITIATE TRANSFER OF EXISTING DATA IF REPLICATION WAS SUCCESSFUL.
	#
	if [ $RESULT_VAL -eq 0 ] 
	then
		echo "[ERROR] Replication could not be enabled on table '$user_table' due to following error(s):"
		grep "ERROR:" /tmp/hbase.out  | sed -e 's/^/[ERROR] /g'
	else
		echo "[INFO] Replication enabled successfully on table '$user_table' at timestamp '$CUR_TIMESTAMP'."

		# MIGRATE THE EXISTING DATA IF USER WANTS TO. 
		#
		if [[ $MIGRATE_EXISTING_DATA == true ]]
		then
			# SAVE THE TIMESTAMP IN TABLE_TS_MAP.
			#
			TABLE_TS_MAP[$user_table]=$CUR_TIMESTAMP

		fi
	fi
done


#------------------------------------------------------------------
# PERFORM MIGRATION OF EXISTING DATA IF REQUESTED BY USER
#------------------------------------------------------------------

TABLE_COPY_STRING=

if [[ $MIGRATE_EXISTING_DATA == true ]]
then

	for K in "${!TABLE_TS_MAP[@]}"
	do
		CURRENT_TABLE=$K
		END_TS=${TABLE_TS_MAP[$K]}
		
		if [[ -z $TABLE_COPY_STRING ]]
		then
			TABLE_COPY_STRING="$CURRENT_TABLE#0#$END_TS"
		else
			TABLE_COPY_STRING="$TABLE_COPY_STRING;$CURRENT_TABLE#0#$END_TS"
		fi 

	done

	echo $TABLE_COPY_STRING

	# DOWNLOAD hdi_copy_table.sh script
	#
	echo "[INFO] Downloading https://raw.githubusercontent.com/Azure/hbase-utils/gkanade-fixrepforcustomnamespace/replication/nohup_hdi_copy_table.sh script to /tmp directory"
	
	wget https://raw.githubusercontent.com/Azure/hbase-utils/gkanade-fixrepforcustomnamespace/replication/nohup_hdi_copy_table.sh -O /tmp/nohup_hdi_copy_table.sh

	chmod +x /tmp/nohup_hdi_copy_table.sh

	echo '[INFO] Running /tmp/nohup_hdi_copy_table.sh -t $TABLE_COPY_STRING -p $REPLICATION_PEER'

	/tmp/nohup_hdi_copy_table.sh -t "$TABLE_COPY_STRING" -p "$REPLICATION_PEER" -m $MACHINE
fi


