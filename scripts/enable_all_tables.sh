#!/bin/bash

#-------------------------------------------------------------------------------#
# SCRIPT TO ENABLE ALL HBASE TABLES.
#-------------------------------------------------------------------------------#

LIST_OF_TABLES=/tmp/tables.txt
HBASE_SCRIPT=/tmp/hbase_script.txt
TARGET_HOST=$1

usage ()
{
	if [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]
	then
		cat << ...

Usage: 

$0 [hostname]

	Note: Providing hostname is optional and not required when script 
	is executed within HDInsight cluster with access to 'hbase shell'.

	However host name should be provided when executing the script as 
	script-action from HDInsight portal.

For Example:

	1.	Executing script inside HDInsight cluster (where 'hbase shell' is 
		accessible):

		$0 

		[No need to provide hostname]

	2.	Executing script from HDinsight Azure portal:

		Provide Script URL.

		Provide hostname as a parameter (i.e. hn0, hn1 or wn2 etc.).
...
		exit
	fi
}

validate_machine ()
{
	THIS_HOST=`hostname`

	if [[ ! -z "$TARGET_HOST" ]] && [[ $THIS_HOST  != $TARGET_HOST* ]]
	then
		echo "[INFO] This machine '$THIS_HOST' is not the right machine ($TARGET_HOST) to execute the script."
		exit 0
	fi
}

get_tables_list ()
{
hbase shell << ... > $LIST_OF_TABLES 2> /dev/null
	list
	exit
...
}

add_table_for_enable ()
{
	TABLE_NAME=$1
	echo "[INFO] Adding table '$TABLE_NAME' to enable list..."
	cat << ... >> $HBASE_SCRIPT
		enable '$TABLE_NAME'
...
}

clean_up ()
{
	rm -f $LIST_OF_TABLES
	rm -f $HBASE_SCRIPT
}

########
# MAIN #
########

usage $1

validate_machine

clean_up

get_tables_list

START=false

while read LINE 
do 
	if [[ $LINE == TABLE ]] 
	then
		START=true
		continue
	elif [[ $LINE == *row*in*seconds ]]
	then
		break
	elif [[ $START == true ]]
	then
		add_table_for_enable $LINE
	fi

done < $LIST_OF_TABLES

cat $HBASE_SCRIPT

hbase shell $HBASE_SCRIPT << ... 2> /dev/null
exit
...

