#!/bin/bash

#-------------------------------------------------------------------------------------------------------
# THIS SCRIPT HELPS IN COPYING A LIST OF TABLES FOR A GIVEN TIME RANGE TO ANOTHER HBASE CLUSTER
#-------------------------------------------------------------------------------------------------------


#----------------------------------------------------------------
# PRINT USAGE INFORMATION
#----------------------------------------------------------------

print_usage()
{
cat << ...
Usage: 
$0 -t <table1:start_timestamp:end_timestamp;table2:start_timestamp:end_timestamp;...> -p <replication_peer> [-m <hostname>] [-everythingTillNow]

Mandatory arguments:
--------------------

-t, --table-list         

					A double-quoted, ';' separated list of tables along with the start and end 
					timestamp range which needs to be copied. For each table, the tablename, 
					starttime and endtime should be separated by ':'. 

					If the list of tables is too large and exceed the limitation of command line
					argument, then this command multiple times with limited tables.

					For example: 

							-t "table1:0:452256397;table2:14141444:452256397"
									OR
							--tablelist="table1:0:452256397;table2:14141444:452256397"

-p, --replication-peer               

					This is the zookeeper peer address of HBase cluster where table needs to be
					copied. 

					For example: 

							-p zk5-hbrpl2;zk1-hbrpl2;zk5-hbrpl2:2181:/hbase-unsecure
									OR
							--replication-peer=dsthbcluster


-m, --machine            

					This option should be used when running the $0 script as 
					Script Action from HDInsight portal or Azure Powershell.
					It is recommended to set -m as hn1 which is usually idle.


-everythingTillNow
				
					Use this switch when user does not want to compute start/end timestamps. 
					This switch will copy all rows until current system timestamp.

-h, --help               

				 	Display's usage information.

...
exit 132
}

#------------------------------------------------------------------
# INITIALIZE PARAMETERS
#------------------------------------------------------------------

TABLE_LIST=
REPLICAITON_PEER=
TARGET_MACHINE=`hostname`
EVERYTHING_TILL_NOW=false

#------------------------------------------------------------------
# PARSE AND PROCESS COMMAND LINE ARGUMENTS
#------------------------------------------------------------------

process_arguments()
{
	while :; do
		case $1 in
			-h|--help)  
				print_usage
				exit 0
				;;

			-t|--table-list)  
				if [ -n "$2" ]; then
					TABLE_LIST=$2
					shift
				else
					printf '[ERROR] -t or --table-list requires non-empty list of tables along with start and end timestamps.' >&2
					print_usage
					exit 1
				fi
				;;

			--table-list=?*)
				TABLE_LIST=${1#*=} 
				;;

			--table-list=)
				# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -t or --table-list requires non-empty list of tables along with start and end timestamps.' >&2
				print_usage
				exit 1
				;;

			-p|--replication-peer)  
				if [ -n "$2" ]; then
					REPLICAITON_PEER=$2
					shift
				else
					printf '[ERROR] -p or --replication-peer requires non-empty value.' >&2
					print_usage
					exit 1
				fi
				;;

			--replication-peer=?*)
				REPLICAITON_PEER=${1#*=} 
				;;

			--replication-peer=)
				# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -p or --replication-peer requires non-empty value.' >&2
				print_usage
				exit 1
				;;

			-m|--machine)  
				if [ -n "$2" ]; then
					TARGET_MACHINE=$2
					shift
				else
					printf '[ERROR] -m or --machine requires non-empty machine name.' >&2
					print_usage
					exit 1
				fi
				;;

			-everythingTillNow)  
				EVERYTHING_TILL_NOW=true
				;;

			--machine=?*)
				TARGET_MACHINE=${1#*=} 
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
}

#------------------------------------------------------------------
# VALIDATE MANDATORY COMMAND LINE PARAMETERS
#------------------------------------------------------------------

validate_arguments()
{
	if [[ -z "${TABLE_LIST// }" ]] || [[ -z "${REPLICAITON_PEER// }" ]]; then
		printf '[ERROR] Mandatory arguments missing.\n' >&2
		print_usage
		exit 1
	fi

	if [[ $TARGET_MACHINE != hn* ]]; then
		printf '[ERROR] -m accepts only hn0 or hn1 as arguments.\n' >&2
		print_usage
		exit 1
	fi

	# MACHINE VALIDATION.
	#
	THIS_MACHINE=`hostname`

	if [[ $THIS_MACHINE != $TARGET_MACHINE* ]]; then
		printf '[ERROR] Not the correct machine to execute the script. Exiting!\n' >&2
		exit 0
	fi
}

copy_tables()
{
	TABLES_ARRAY=(`echo $TABLE_LIST | sed -e 's/;/ /g'`)

	if [[ $EVERYTHING_TILL_NOW == true ]];
	then
		START_TIME=0
		END_TIME=$(($(date +%s%N)/1000000))
	fi


	for TABLE_ENTRY in "${TABLES_ARRAY[@]}" 
	do
		TABLE_NAME=`echo $TABLE_ENTRY | cut -f 1 -d ':'`
		
		if [[ $EVERYTHING_TILL_NOW == false ]]
		then
			START_TIME=`echo $TABLE_ENTRY | cut -f 2 -d ':'`
			END_TIME=`echo $TABLE_ENTRY | cut -f 3 -d ':'`
			
			REGEX='^[0-9]+$'
			if ! [[ $START_TIME =~ $REGEX ]];
			then
				echo "[ERROR] Start time '$START_TIME' for table '$TABLE_NAME' is not a valid number."
				exit 1
			fi

			if ! [[ $END_TIME =~ $REGEX ]];
			then
				echo "[ERROR] End time '$END_TIME' for table '$TABLE_NAME' is not a valid number."
				exit 1
			fi

			if  [ $START_TIME -gt $END_TIME ];
			then
				echo "[ERROR] Start time '$START_TIME' for table '$TABLE_NAME' is greater than end time '$END_TIME'."
				exit 1
			fi
		fi

		echo "[INFO] Transferring pre-existing data of table '$CURRENT_TABLE' upto END_TIMESTAMP='$END_TS'"
		echo "[INFO] Running command: 'hbase org.apache.hadoop.hbase.mapreduce.CopyTable --peer.adr=$REPLICAITON_PEER --starttime=$START_TIME --endtime=$END_TIME $TABLE_NAME'"
		hbase org.apache.hadoop.hbase.mapreduce.CopyTable --peer.adr=$REPLICAITON_PEER --starttime=$START_TIME --endtime=$END_TIME $TABLE_NAME
	done
}

#------------------------------------------------------------------
#  MAIN
#------------------------------------------------------------------

process_arguments $@

validate_arguments

copy_tables
