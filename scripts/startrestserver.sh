#! /bin/bash

THIS_MACHINE=`hostname`

if [[ $THIS_MACHINE != wn* ]]
then
	printf 'Script to be executed only on worker nodes'
	exit 0
fi

RESULT=`pgrep -f RESTServer`
if [[ -z $RESULT ]]
then
	echo "Applying mitigation; starting REST Server"
	sudo python /usr/lib/python2.7/dist-packages/hdinsight_hbrest/HbaseRestAgent.py
else
	echo "Rest server already running"
	exit 0
fi
