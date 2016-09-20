#!/bin/bash 

#-----------------------------------------------------------------------------------#
# This script analyzes the Azure storage metrics and prints the Avg., 95 percentile 
# and 99 percentile scores for BLOB I/O operations.									
#-----------------------------------------------------------------------------------#


INPUT_DIR=

OPERATIONS_LIST=

OPERATIONS_ARRAY=()

print_usage()
{
cat << ...
	
Usage: 
	$0 <directory> [blob_operations]
		
	<directory> - The command takes full path to the directory containing hourly logs. 
				For example: "/d/storage_logs/2016/09/24"
				This is mandatory argument.

	[operations] - A ',' separated list of blob operations. 
				For example: "PutPage,PutBlock,GetPage,GetBlock"
				This argument is optional and by default, the analysis is performed
				for all operations.

For Example:
	
	$0 "/d/storage_logs/2016/09/24"  
	
	The above command will analyze the storage metrics for date 09-24-2016 and print them on hourly basis.
...

exit 1
}

process_arguments ()
{
	
	if [[ -z $1 ]] || [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]
	then
		print_usage
	fi

	INPUT_DIR="$1"
	
	# VALIDATE

	ls "$INPUT_DIR" > /dev/null 2>&1

	RESULT=$?

	if [ $RESULT -ne 0 ]
	then
		print_usage
	fi


	if [[ ! -z $2 ]]
	then
		TEMP_IFS=$IFS
		IFS=',' read -ra OPERATIONS_ARRAY <<< "$2"
		IFS=$TEMP_IFS
	else
		TEMP_IFS=$IFS
		IFS=',' read -ra OPERATIONS_ARRAY <<< "PutPage,PutBlock,GetPage,GetBlock"
		IFS=$TEMP_IFS
	fi


}

print_analysis ()
{
		OPERATION=$1 

		echo "----------------------------"
		echo "$OPERATION Analysis:"
		echo "----------------------------"

		echo "Hour TotalWrites MinLatency MaxLatency MedianLatency AvgLatency 95%tileLatency 99%tileLatency MinBytes MaxBytes MedianBytes AvgBytes 95%tileBytes 99%tileBytes"

		for dir in `ls -1`
		do
				let sum=0
				for file in `ls -1 $dir/` 
				do 
					if [[ $OPERATION != ServerTimeoutError ]] 
					then
						lines=`cat $dir/$file | grep -v "ServerTimeoutError" | grep  ";$OPERATION;" | wc -l`
						let sum=$sum+$lines
					else
						lines=`cat $dir/$file | grep  ";$OPERATION;" | wc -l`
						let sum=$sum+$lines
					fi
				done
				echo -n "$dir $sum "

				if [[ $OPERATION != ServerTimeoutError ]] 
				then
						# ----------------------------------------------------------------------------------
						# Some URL's have ';', so we need to mask any string inside '"' that might contain ;
						# Also, we need to account successful v/s unsuccessful IOPS separately.
						# ----------------------------------------------------------------------------------
						#
						grep ";$OPERATION;" $dir/*.log |  grep -v "ServerTimeoutError" |  sed -e 's/;"[^"]*";/;"";/g' | cut -f 6 -d ';' | sort -n | awk -v ORS=" " '{a[i++]=$0;s+=$0}END{print a[0],a[i-1],(a[int(i/2)]+a[int((i-1)/2)])/2,int(s/i),a[int(i-((i*5)/100))],a[int(i-(i/100))]}';
						grep ";$OPERATION;" $dir/*.log | grep -v "ServerTimeoutError" |  sed -e 's/;"[^"]*";/;"";/g' | cut -f 19 -d ';' | sort -n | awk '{a[i++]=$0;s+=$0}END{print a[0],a[i-1],(a[int(i/2)]+a[int((i-1)/2)])/2,int(s/i),a[int(i-((i*5)/100))],a[int(i-(i/100))]}';
				else
						grep ";$OPERATION;" $dir/*.log |  sed -e 's/;"[^"]*";/;"";/g' |  cut -f 6 -d ';' | sort -n | awk -v ORS=" " '{a[i++]=$0;s+=$0}END{print a[0],a[i-1],(a[int(i/2)]+a[int((i-1)/2)])/2,int(s/i),a[int(i-((i*5)/100))],a[int(i-(i/100))]}';
						grep ";$OPERATION;" $dir/*.log |   sed -e 's/;"[^"]*";/;"";/g' | cut -f 19 -d ';' | sort -n | awk '{a[i++]=$0;s+=$0}END{print a[0],a[i-1],(a[int(i/2)]+a[int((i-1)/2)])/2,int(s/i),a[int(i-((i*5)/100))],a[int(i-(i/100))]}';
				fi
		done
}

#-----------------
# MAIN 
#-----------------

process_arguments "$1" "$2" 

pushd "$INPUT_DIR"

for BLOB_OPERATION in "${OPERATIONS_ARRAY[@]}"
do
	print_analysis $BLOB_OPERATION
done

print_analysis "ServerTimeoutError"

popd
