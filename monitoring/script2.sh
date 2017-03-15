#!/bin/bash
wget https://github.com/Microsoft/OMS-Agent-for-Linux/releases/download/OMSAgent-201702-v1.3.1-15/omsagent-1.3.1-15.universal.x64.sh -O /tmp/omsagent.x64.sh
sudo sh /tmp/omsagent.x64.sh --upgrade
sudo sh -x /opt/microsoft/omsagent/bin/omsadmin.sh -w $1 -s $2

if [[ $HOSTNAME == hn* ]];
then
  sudo wget https://raw.githubusercontent.com/Azure/hbase-utils/master/monitoring/yarn.headnode.conf -O /etc/opt/microsoft/omsagent/conf/omsagent.d/yarn.headnode.conf
elif [[ $HOSTNAME == wn* ]];
then
  sudo wget https://raw.githubusercontent.com/Azure/hbase-utils/master/monitoring/yarn.workernode.conf -O /etc/opt/microsoft/omsagent/conf/omsagent.d/yarn.workernode.conf
  sudo wget https://raw.githubusercontent.com/Azure/hbase-utils/master/monitoring/hbase.workernode.conf -O /etc/opt/microsoft/omsagent/conf/omsagent.d/hbase.workernode.conf
elif [[ $HOSTNAME == zk* ]];
then
  sudo wget https://raw.githubusercontent.com/Azure/hbase-utils/master/monitoring/hbase.zookeeper.conf -O /etc/opt/microsoft/omsagent/conf/omsagent.d/hbase.zookeeper.conf
fi

sudo /opt/microsoft/omsagent/bin/service_control restart
