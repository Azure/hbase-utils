
INTRODUCTION
--------------

This repository contains a script to enable replication between two HDInsight HBase clusters in easiest possible way.
Details on how to install and use the hdi_enable_replication.sh are provided below.

A sample workload has also been provided to test the HBase replication.


INSTALLATION
--------------

Download hdi_enable_replication.sh on the head node of the source (primary) cluster.

Optionally,
  1) Download sample-workload/src-cluster/* on source (primary) cluster.
  2) Download sample-workload/dst-cluster/* on sink (replica) cluster.
  3) On primary cluster, edit STEP3.sh script and change the cluster names and credentials.
  4) Run STEP[1-7].sh scripts in numerical order (you would need to go back and forth between
    two clusters.
    The scripts are self explanatory.


USAGE
-------------

./hdi_enable_replication.sh -s <src_cluster_dns> -sp <src_ambari_password> -d <dst_cluster_dns> -dp <dst_ambari_password> [optional arguments]

Mandatory arguments:
--------------------

-s, --src-cluster              
                                DNS name of the source HBase cluster.
                                For example: 
                                -s hbsrccluster
                                --src-cluster=hbsrccluster

-d, --dst-cluster               
                                DNS name of the destination (replica) HBase cluster.
                                For example: 
                                -s dsthbcluster
                                --src-cluster=dsthbcluster

-sp, --src-ambari-password      
                                Admin password for Ambari of source HBase cluster.

-dp, --dst-ambari-password      
                                Admin password for Ambari of destination HBase cluster.

Optinal arguments:
------------------

-su, --src-ambari-user          
                                Admin username for Ambari of source HBase cluster.
                                Default = admin.

-du, --dst-ambari-user          
                                Admin username for Ambari of destination HBase cluster.
                                Default = admin.

-t, --table-list                
                                ';' separated list of tables to be replicated. 
                                
                                For example: --table-list="table1;table2;table3"
                                By default - all hbase tables are replicated.

-m, --machine                   
                                This option should be used when running the $0 script as 
                                Script Action from HDInsight portal or Azure Powershell.
                                the value of -m should be either hn0 or hn1 for HDI HBase
                                clusters.
 
-ip								
                                This argument acts as a switch to utilize the static IP's of zookeeper
                                nodes from replica cluster instead of FQDN names. The static IP's 
                                needs to be pre-configured before enabling replication. 
                                This argument is mandatory when enabling replication across two 
                                different VNET's.

-cp, -copydata
                                This option is a switch which enables the migration of 
                                existing data on the tables where replication gets enabled.

-rpm, -replicate-phoenix-meta
                                This switch enables the replication on Phoenix system (SYSTEM.*)
                                tables. 

                                NOTE: This option needs to be used with caution!
                                It is in general advised to recreate phoenix tables on replica
                                cluster before using this script. 

-h, --help                      
                                Display's usage information.

Sample Commands:
------------------

1) To enable replication on all tables without migrating existing data:

   $0 -s pri-hbcluster -d sec-hbcluster -sp Mypassword\!789 -dp Mypassword1234#
 
2) To enable replication on tables specified (table1, table2 and table3) and also migrating the existing data, use following command:

   $0 --src-cluster=pri-hbcluster --dst-cluster=sec-hbcluster --src-ambari-user=admin --src-ambari-password=Hello\!789 --dst-ambari-user=admin --dst-ambari-password=Sample1234# --table-list="table1;table2;table3" -cp



