
INTRODUCTION
--------------

This repository contains a script to enable replication between two HDInsight HBase clusters in easiest possible way.
Details on how to install and use the enable_replication.sh are provided below.

A sample workload has also been provided to test the HBase replication.


INSTALLATION
--------------

Download enable_replication.sh on the head node of the source (primary) cluster.

Optionally,
  1) Download sample-workload/src-cluster/* on source (primary) cluster.
  2) Download sample-workload/dst-cluster/* on sink (replica) cluster.
  3) On primary cluster, edit STEP3.sh script and change the cluster names and credentials.
  4) Run STEP[1-7].sh scripts in numerical order (you would need to go back and forth between
    two clusters.
    The scripts are self explanatory.


USAGE
-------------

Usage:
./enable_replication.sh -s <src_cluster_dns> -sp <src_ambari_password> -d <dst_cluster_dns> -dp <dst_ambari_password> [optional arguments]

Mandatory arguments:
--------------------

-s, --src-cluster               DNS name of the source HBase cluster.
                                For example:
                                -s hbsrccluster
                                --src-cluster=hbsrccluster

-d, --dst-cluster               DNS name of the destination (replica) HBase cluster.
                                For example:
                                -s dsthbcluster
                                --src-cluster=dsthbcluster

-sp, --src-ambari-password      Admin password for Ambari of source HBase cluster.

-du, --dst-ambari-user          Admin username for Ambari of destination HBase cluster.

-dp, --dst-ambari-password      Admin password for Ambari of destination HBase cluster.

Optinal arguments:
------------------

-su, --src-ambari-user          Admin username for Ambari of source HBase cluster.
                                Default = admin.

-t, --table-list                ';' separated list of tables to be replicated.
                                For example: --table-list="table1;table2;table3"
                                By default - all hbase tables are replicated.

-h, --help                      Display's usage information.

Sample Commands:
------------------

./enable_replication.sh -s pri-hbcluster -d sec-hbcluster -sp Mypassword\!789 -dp Mypassword1234#

./enable_replication.sh --src-cluster=pri-hbcluster --dst-cluster=sec-hbcluster --src-ambari-user=admin --src-ambari-password=Hello\!789 --dst-ambari-user=admin --dst-ambari-password=Sample1234# --table-list="table1;table2;table3"


