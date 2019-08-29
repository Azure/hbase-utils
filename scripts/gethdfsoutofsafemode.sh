#/bin/bash
result=$(hdfs dfsadmin -fs hdfs://mycluster/ -safemode leave | grep "Safe mode is OFF")
if [[ -z $result ]]; then
	echo "Unable to quit safe mode, please contact support"
else
	hdfs fsck hdfs://mycluster/ | grep 'Under replicated' | awk -F':' '{print $1}' >> /tmp/under_replicated_files
	for hdfsfile in `cat /tmp/under_replicated_files`; do echo "Fixing $hdfsfile: " ; hadoop fs -setrep 3 $hdfsfile; done
	hdfs fsck hdfs://mycluster/ | grep 'MISSING' | awk -F':' '{print $1}' >> /tmp/missing_files
	for hdfsfile in `cat /tmp/missing_files` ; do echo "Removing $hdfsfile: " ; hadoop fs -fs hdfs://mycluster/ -rm $hdfsfile; done 

