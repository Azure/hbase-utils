#! /bin/bash

#-------------------------------------------------------------------------------------
# THIS SCRIPT COLLECTS JSTACKS ON ALL OR SPECIFIED JAVA PROCESSES IN HDINSIGHT CLUSTER
#-------------------------------------------------------------------------------------

#----------------------------------------------------------------
# PRINT USAGE INFORMATION
#----------------------------------------------------------------

print_usage()
{
cat << ...
Usage: 

$0 -d <total collection time (sec)> -i <sampling interval (sec)> [-p <list of java processes separated by ';'>]

		The command collect the stacks on local file system for specified time 
		duration. After completion, the data is compressed and moved to HDFS at 
		/jstack location. 

Sample Commands:
------------------

1) Collecting jstacks on all java processes for 1 hour and with sampling interval of 10 seconds:

$0 -d 3600 -i 10 

2) Collecting jstacks on PQS and RegionServer processes for 10 hours, with sampling interval of 2 seconds:

$0 -d 36000 -i 2 -p "proc_regionserver;proc_phoenixserver" 

...

exit 132
}

#------------------------------------------------------------------
# INITIALIZE PARAMETERS
#------------------------------------------------------------------

TOTAL_DURATION=
SAMPLING_INTERVAL=
LIST_OF_JAVA_APPS=

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

			-d|--duration)  
				if [ -n "$2" ]; then
					TOTAL_DURATION=$2
					shift
				else
					printf '[ERROR] -d or --duration requires non-empty time duration in seconds.' >&2
					print_usage
					exit 1
				fi
				;;

			--duration=?*)
				TOTAL_DURATION=${1#*=} 
				;;

			--duration=)
			# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -d or --duration requires non-empty time duration in seconds.' >&2
				print_usage
				exit 1
				;;

			-i|--interval)  
				if [ -n "$2" ]; then
					SAMPLING_INTERVAL=$2
					shift
				else
					printf '[ERROR] -i or --interval requires non-empty sampling interval in seconds.' >&2
					print_usage
					exit 1
				fi
				;;

			--interval=?*)
				SAMPLING_INTERVAL=${1#*=} 
				;;

			--interval=)
			# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -i or --interval requires non-empty sampling interval in seconds.' >&2
				print_usage
				exit 1
				;;

			-j|--java_processes)  
				if [ -n "$2" ]; then
					LIST_OF_JAVA_APPS=$2
					shift
				else
					printf '[ERROR] -j or --java_processes requires non-empty list of java processes.' >&2
					print_usage
					exit 1
				fi
				;;

			--java_processes=?*)
				LIST_OF_JAVA_APPS=${1#*=} 
				;;

			--java_processes=)
			# Handle the case where no argument is specified after '=' sign.
				printf '[ERROR] -j or --java_processes requires non-empty list of java processes.' >&2
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

	echo "[INFO] Commandline arguments successfully processed."
}

#------------------------------------------------------------------
# VALIDATE MANDATORY COMMAND LINE PARAMETERS
#------------------------------------------------------------------

validate_arguments()
{
	# VALIDATE MANDATORY ARGUMENTS
	#
	if [[ -z "${TOTAL_DURATION// }" ]] || [[ -z "${SAMPLING_INTERVAL// }" ]]; then
		printf '[ERROR] Mandatory arguments missing.\n' >&2
		print_usage
		exit 1
	fi

	echo "[INFO] Commandline arguments successfully validated"
}

#------------------------------------------------------------------
# SET ARRAY OF JAVA PROCESSES FOR JSTACK COLLECTION
#------------------------------------------------------------------

JVM_ARRAY=()
build_array_of_java_processes ()
{
	# IF USER PROVIDES A LIST OF JAVA PROCESSES, USE THOSE.
	# OTHERWISE, PERFORM JSTACK COLLECTION ON ALL JAVA PROCESSES
	# CONTAINING -Dproc_<processname>.
	#
	if [[ ! -z $LIST_OF_JAVA_APPS ]]
	then
		JVM_ARRAY=(`echo $LIST_OF_JAVA_APPS | sed -e 's/;/ /g'`)
	else
		JVM_ARRAY=(`sudo ps -afe | grep "\-Dproc_" | grep -v grep | sed -e 's/^.*\-D\(proc_[^ ][^ ]*\).*/\1/g'`)
	fi

	echo -n "[INFO] Starting JStack collection for these JVM processes - "
	(IFS=$', '; echo "${JVM_ARRAY[*]}" )
	echo ""
}

ensure_root_access()
{
	THIS_USER=`whoami`
	if [[ ! $THIS_USER = root ]]; then
		echo "[ERROR] This script needs to be executed as root."
		exit 1
	fi
}

COLLECTION_DIR=/var/log/jstack/$HOSTNAME
setup_collection_dir()
{
	if [ -d $COLLECTION_DIR ]
	then 
		rm -fr $COLLECTION_DIR
	fi

	mkdir -p $COLLECTION_DIR 
	RESULT=$?
	if [ "$RESULT" -ne "0" ]
	then
		echo "[ERROR] Can't create COLLECTION_DIR=$COLLECTION_DIR."
		exit 1
	fi
	
	echo "[INFO] Collection directory '$COLLECTION_DIR' successfully created."
}

begin_collection()
{
	START_TIME=`date +%s`
	ELAPSED_TIME=0

	# ERROR HANDLING.
	# 
	ERROR_COUNT=0	

	CURRENT_ITERATION=0

	# LOOP WHILE TOTAL DURATION IS NOT EXHAUSTED
	#
	while [ $ELAPSED_TIME -lt $TOTAL_DURATION ];
	do
		echo "[INFO] JStack collection iteration = $CURRENT_ITERATION."

		# FOR EACH JVM PROCESS
		# 	1) EXTRACT USER AND PID
		#	2) COLLECT JSTACK TO COLLECTION_DIR
		# 	3) TAG JSTACK WITH PID, USER, PROCESS_NAME, CURRENT_TIME
		# 
		# SLEEP FOR SAMPLING_INTERVAL
		#

		CURRENT_ITERATION=$((CURRENT_ITERATION+1))		

		for JVM_PROCESS in "${JVM_ARRAY[@]}"
		do
			THIS_PID=`sudo ps -afe | grep "\-D$JVM_PROCESS" | grep -v grep  | awk '{ print $2 }' | head -1`
			THIS_USER=`sudo ps -afe | grep "\-D$JVM_PROCESS" | grep -v grep  | awk '{ print $1 }' | head -1`
			if [[ -z $THIS_PID ]] || [[ -z $THIS_USER ]]; 
			then 
				let "ERROR_COUNT += 1"
				if [ $ERROR_COUNT -gt 10 ];
				then
					echo "[ERROR] Unable to get PID or USER for process $JVM_PROCESS"
					exit 1
				fi
			fi

			# PREPARE FILE PATH
			#
			FILE_PATH=$COLLECTION_DIR/$JVM_PROCESS.$THIS_PID.$THIS_USER.$CURRENT_ITERATION.`date +"%m-%d-%Y_%T_%Z"`
			su - $THIS_USER -c "jstack $THIS_PID" > $FILE_PATH
		done

		sleep $SAMPLING_INTERVAL

		TIME_NOW=`date +%s`
		ELAPSED_TIME=$((TIME_NOW-START_TIME))

	done

	# COPY TO HDFS	
}

compress_and_package()
{
	tar zcvf $COLLECTION_DIR.tar.gz $COLLECTION_DIR
	RESULT=$?

	if [ "$RESULT" -ne "0" ]
	then 
		echo "[ERROR] Couldn't archive $COLLECTION_DIR."
		exit 1
	else
		echo "[INFO] Successfully packaged jstack collection to $COLLECTION_DIR.tar.gz file"
	fi
}

copy_to_hdfs()
{
	hadoop fs -test -d /jstack
	RESULT=$?
	if [ "$RESULT" -ne "0" ]
	then
		hadoop fs -mkdir -p /jstack
	fi

	hadoop fs -copyFromLocal -f $COLLECTION_DIR.tar.gz /jstack/

	echo "[INFO] Successfully copied $COLLECTION_DIR.tar.gz to /jstack/ HDFS location (Storage Account)."
}


#------------------------------------------------------------------
#  MAIN
#------------------------------------------------------------------

ensure_root_access

process_arguments $@

validate_arguments

build_array_of_java_processes

setup_collection_dir

begin_collection

compress_and_package

copy_to_hdfs


