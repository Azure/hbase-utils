#!/bin/bash

wget https://github.com/Microsoft/OMS-Agent-for-Linux/releases/download/OMSAgent_GA_v1.2.0-25/omsagent-1.2.0-25.universal.x64.sh -O /tmp/omsagent.x64.sh
sudo sh /tmp/omsagent.x64.sh --upgrade -w $1 -s $2

if [[ $HOSTNAME == wn* ]];
then
  sudo wget https://raw.githubusercontent.com/Azure/hbase-utils/master/monitoring/hbase.workernode.conf -O /etc/opt/microsoft/omsagent/conf/omsagent.d/hbase.workernode.conf
elif [[ $HOSTNAME == zk* ]];
then
  sudo wget https://raw.githubusercontent.com/Azure/hbase-utils/master/monitoring/hbase.zookeeper.conf -O /etc/opt/microsoft/omsagent/conf/omsagent.d/hbase.zookeeper.conf
fi

sudo wget https://raw.githubusercontent.com/Azure/hbase-utils/master/monitoring/filter_hdinsight.rb -O /opt/microsoft/omsagent/plugin/filter_hdinsight.rb

sudo wget https://raw.githubusercontent.com/Azure/hbase-utils/master/monitoring/hdinsightmanifestreader.rb -O  /opt/microsoft/omsagent/bin/hdinsightmanifestreader.rb
sudo wget https://raw.githubusercontent.com/Azure/hbase-utils/master/monitoring/omsagent
sudo cp omsagent /etc/sudoers.d/
sudo sh ~/.bashrc
sudo service omsagent restart
