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

-s, --src-cluster               DNS name of the source HBase cluster.
                                For example: 
                                -s hbsrccluster
                                --src-cluster=hbsrccluster

-d, --dst-cluster               DNS name of the destination (replica) HBase cluster.
                                For example: 
                                -s dsthbcluster
                                --src-cluster=dsthbcluster

-sp, --src-ambari-password      Admin password for Ambari of source HBase cluster.

-du, --dst-ambari-user          Admin username for Ambari of destination HBase cluster.

-dp, --dst-ambari-password      Admin password for Ambari of destination HBase cluster.

Optinal arguments:
------------------

-su, --src-ambari-user          Admin username for Ambari of source HBase cluster.
                                Default = admin.

-t, --table-list                ';' separated list of tables to be replicated. 
                                For example: --table-list="table1;table2;table3"
                                By default - all hbase tables are replicated.

-h, --help                      Display's usage information.

Sample Commands:
------------------

$0 -s pri-hbcluster -d sec-hbcluster -sp Mypassword\!789 -dp Mypassword1234#
 
$0 --src-cluster=pri-hbcluster --dst-cluster=sec-hbcluster --src-ambari-user=admin --src-ambari-password=Hello\!789 --dst-ambari-user=admin --dst-ambari-password=Sample1234# --table-list="table1;table2;table3"

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
				if [ -n "$2" ]; then
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
				if [ -n "$2" ]; then
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
				if [ -n "$2" ]; then
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

			-t|--table-list)  
				if [ -n "$2" ]; then
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
	if [[ -z "${SRC_CLUSTER// }" ]] || [[ -z "${SRC_AMBARI_PASSWORD// }" ]] || [[ -z "${DST_CLUSTER// }" ]] || [[ -z "${DST_AMBARI_PASSWORD// }" ]]; then
		printf '[ERROR] Mandatory arguments missing.\n' >&2
		print_usage
		exit 1
	fi
}

#------------------------------------------------------------------
# VALIDATE AMBARI CREDENTIALS
#------------------------------------------------------------------

validate_ambari_credentials() 
{
	SRC_AMBARI_PASSWORD=`echo $SRC_AMBARI_PASSWORD`

	curl -u $SRC_AMBARI_USER:$SRC_AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$SRC_CLUSTER.azurehdinsight.net/api/v1/clusters/$SRC_CLUSTER?fields=Clusters/desired_configs/hbase-site" -o /tmp/hbase.json 2> /dev/null
	grep -i "access.*denied" /tmp/hbase.json > /dev/null 2>&1

	RESULT=$?
	if [ $RESULT -eq 0 ]; then
		echo "[ERROR] Invalid Ambari username or password for cluster $SRC_CLUSTER. Exiting!"
		cat /tmp/hbase.json | sed -e 's/^/[INFO] /g'
		exit 134
	else
		echo "[INFO] Primary cluster credentials successfully validated."
	fi

	curl -u $DST_AMBARI_USER:$DST_AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$DST_CLUSTER.azurehdinsight.net/api/v1/clusters/$DST_CLUSTER?fields=Clusters/desired_configs/hbase-site" -o /tmp/hbase.json 2> /dev/null
	grep -i "access.*denied" /tmp/hbase.json > /dev/null 2>&1

	RESULT=$?
	if [ $RESULT -eq 0 ]; then
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
	curl -u $DST_AMBARI_USER:$DST_AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$DST_CLUSTER.azurehdinsight.net/api/v1/clusters/$DST_CLUSTER?fields=Clusters/desired_configs/hbase-site" -o /tmp/hbase.json 2> /dev/null
	grep tag /tmp/hbase.json > /dev/null

	if (( $? !=  0 )) ; then
		echo "[ERROR] Could not set replication peer."
		echo "[ERROR] Curl command failed to query '$DST_CLUSTER' due to following reason:"
		cat /tmp/hbase.json  | sed -e 's/^/[INFO] /g'
		exit 1
	fi

	local VERSIONTAG=`grep tag /tmp/hbase.json  | awk '{ print $3 }' | sed -e 's/"//g' | sed -e 's/,.*//g'`
	curl -u $DST_AMBARI_USER:$DST_AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$DST_CLUSTER.azurehdinsight.net/api/v1/clusters/$DST_CLUSTER/configurations?type=hbase-site&tag=$VERSIONTAG" -o /tmp/hbase.json 2> /dev/null

	local TEMPQUORUM=`cat /tmp/hbase.json | grep "hbase.zookeeper.quorum" | awk '{ print $3 }'`
	local TEMPPORT=`cat /tmp/hbase.json | grep "hbase.zookeeper.property.clientPort" | awk '{ print $3 }'`
	local TEMPPARENT=`cat /tmp/hbase.json | grep "zookeeper.znode.parent" | awk '{ print $3 }'`

	local ZKQUORUM=`echo ${TEMPQUORUM} | sed -e 's/"//g' | sed -e 's/,$//g'`
	local ZKPORT=`echo ${TEMPPORT} | sed -e 's/"//g' | sed -e 's/,$//g'`
	local ZKPARENT=`echo ${TEMPPARENT} | sed -e 's/"//g' | sed -e 's/,$//g'`

	REPLICATION_PEER=$ZKQUORUM:$ZKPORT:$ZKPARENT

	echo "[INFO] Identified sink cluster peer address as: $REPLICATION_PEER."

	hbase shell << ... 2> /dev/null | sed -e 's/^/[INFO] /g'
	remove_peer '1'
	list_peers
	add_peer '1', "$REPLICATION_PEER"
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
		TABLES_ARRAY=(`curl -u $SRC_AMBARI_USER:$SRC_AMBARI_PASSWORD -G "https://$SRC_CLUSTER.azurehdinsight.net/hbaserest/" 2> /dev/null`)
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

for user_table in "${TABLES_ARRAY[@]}"
do
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

	# CHECK FOR ERRORS.
	#
	grep "ERROR:" /tmp/hbase.out > /dev/null 2>&1 

	RESULT_VAL=$?

	# VALIDATE WHETHER REPLICATION WAS ENABLED SUCCESSFULLY OR NOT.
	# INITIATE TRANSFER OF EXISTING DATA IF REPLICATION WAS SUCCESSFUL.
	#
	if [ $RESULT_VAL -eq 0 ]; then
		echo "[ERROR] Replication could not be enabled on table '$user_table' due to following error(s):"
		grep "ERROR:" /tmp/hbase.out  | sed -e 's/^/[ERROR] /g'
	else
		echo "[INFO] Replication enabled successfully on table '$user_table'."

		CUR_TIMESTAMP=$(($(date +%s%N)/1000000))
		echo "[INFO] Transferring pre-existing data of table '$user_table' upto END_TIMESTAMP=$CUR_TIMESTAMP."

		echo "[INFO] Running command: 'hbase org.apache.hadoop.hbase.mapreduce.CopyTable --peer.adr=$REPLICATION_PEER --endtime=$CUR_TIMESTAMP $user_table'"
		hbase org.apache.hadoop.hbase.mapreduce.CopyTable --peer.adr=$REPLICATION_PEER --endtime=$CUR_TIMESTAMP $user_table > /dev/null 2>&1
	fi

done


