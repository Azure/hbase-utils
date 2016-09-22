#! /bin/bash

#------------------------------------------------------------------
# THIS SCRIPT DISABLES HBASE REPLICATION ON SPECIFIED TABLES
#------------------------------------------------------------------

#----------------------------------------------------------------
# PRINT USAGE INFORMATION
#----------------------------------------------------------------

print_usage()
{
cat << ...
Usage: 
$0 -s <src_cluster_dns> -sp <src_ambari_password> <-all|-t "table1;table2;..."> [optional arguments]

Mandatory arguments:
--------------------

-s, --src-cluster               

                        DNS name of the source HBase cluster.
                        For example: 
                        -s hbsrccluster
                        --src-cluster=hbsrccluster

-sp, --src-ambari-password      

                        Admin password for Ambari of source HBase cluster.

-all | -t, --table-list			

                        If '-all' switch is specified, the replication is 
                        disabled on all the tables.

                        -t (or --table-list) option takes a list of tables 
                        where replication needs to be disabled. The tables 
                        must be separated by ';'. 
                        For Example: --table-list "table1;table2;table3"
								
Optinal arguments:
------------------

-su, --src-ambari-user          

                        Admin username for Ambari of source HBase cluster.
                        Default = admin.

-t, --table-list                

                        ';' separated list of tables to be replicated. 
                        For example: --table-list="table1;table2;table3"
                        By default - all hbase tables are replicated.

-m, --machine            

                        This option should be used when running the $0 script as 
                        Script Action from HDInsight portal or Azure Powershell.
                        the value of -m should be either hn0 or hn1.

-h, --help                   

                        Display's usage information.

Sample Commands:
------------------

$0 -s pri-hbcluster -sp Mypassword\!789 -all
 
$0 --src-cluster=pri-hbcluster --dst-cluster=sec-hbcluster --src-ambari-user=admin \
    --src-ambari-password=Hello\!789 --table-list="table1;table2;table3"

...
exit 132
}

#------------------------------------------------------------------
# INITIALIZE PARAMETERS
#------------------------------------------------------------------

SRC_CLUSTER=
SRC_AMBARI_USER=admin
SRC_AMBARI_PASSWORD=
TABLE_LIST=
AMBARICONFIGS_SH=/var/lib/ambari-server/resources/scripts/configs.sh
PORT=8080
MACHINE=
ALL_TABLES=false

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

			-all)
				ALL_TABLES=true
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
			-m|--machine)  
				if [ -n "$2" ]; then
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
}

#------------------------------------------------------------------
# VALIDATE MANDATORY COMMAND LINE PARAMETERS
#------------------------------------------------------------------

validate_arguments()
{
	if [[ -z "${SRC_CLUSTER// }" ]] || [[ -z "${SRC_AMBARI_PASSWORD// }" ]] ; then
		printf '[ERROR] Mandatory arguments missing.\n' >&2
		print_usage
		exit 1
	fi

	if [[ $ALL_TABLES != true ]] && [[ -z $TABLE_LIST ]] ; then
		printf '[ERROR] Mandatory argument missing. Either -all or -t must be used.\n' >&2
		print_usage
		exit 1
	fi

	if [[ ! -z $MACHINE ]] && [[ $MACHINE != hn* ]]; then
		printf '[ERROR] -m accepts only hn0 or hn1 as arguments.\n' >&2
		exit 1
	fi

	# MACHINE VALIDATION.
	#
	THIS_MACHINE=`hostname`

	if [[ $THIS_MACHINE != $MACHINE* ]]; then
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
}

#------------------------------------------------------------------
# SET ARRAY OF TABLES FOR DISABLING REPLICATION
#------------------------------------------------------------------

set_tables_array ()
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

	# NOTE: VALIDATION OF TABLES IS NOT EASY AS LIST OPERATION COULD TAKE TIME. 
	# FOR CUSTOMER'S WHO HAVE 1000'S OF TABLES, PROVIDING COMMAND LINE ARGUMENT 
	# IS BETTER FOR REPLICATION.
}

list_replicated_tables ()
{
	echo "[INFO] Listing tables where replication has been enabled."
	hbase shell << ... > /tmp/hbase.out 2>&1
	list_replicated_tables
	exit
...
}

#------------------------------------------------------------------
#  MAIN
#------------------------------------------------------------------

process_arguments $@

validate_arguments

validate_ambari_credentials

TABLES_ARRAY=()
set_tables_array 

for user_table in "${TABLES_ARRAY[@]}"
do
	echo "[INFO] Attempting to disable replication for table '$user_table'."

	# DISABLE REPLICATION FROM HBASE SHELL
	#
	hbase shell << ... > /tmp/hbase.out 2>&1
	disable_table_replication '$user_table'
	describe '$user_table'
	exit
...

	# CHECK FOR ERRORS.
	#
	grep "ERROR:" /tmp/hbase.out > /dev/null 2>&1 

	RESULT_VAL=$?

	# VALIDATE WHETHER REPLICATION WAS DISABLED SUCCESSFULLY OR NOT.
	# INITIATE TRANSFER OF EXISTING DATA IF REPLICATION WAS SUCCESSFUL.
	#
	if [ $RESULT_VAL -eq 0 ]; then
		echo "[ERROR] Replication could not be disabled on table '$user_table' due to following error(s):"
		grep "ERROR:" /tmp/hbase.out  | sed -e 's/^/[ERROR] /g'
	else
		echo "[INFO] Replication disabled successfully on table '$user_table'."
	fi

done

list_replicated_tables


