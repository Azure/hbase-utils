INTRODUCTION
--------------

This script adds Zeppelin notebook service to HDInsight HBase Clusters which do not already have it in easiest possible way so that the JDBC Phoenix interpreter is set up and ready to use.

Details on how to install and use the hdi_add_zeppelin.sh are provided below.

INSTALLATION
--------------

Download hdi_add_zeppelin.sh on the head node of your HDI HBase cluster.

USAGE
-------------
./hdi_add_zeppelin.sh -p <Ambari_password> -c <cluster_dns> -h <headnode0_fqdn> [optional arguments]

Mandatory arguments:
--------------------

-c
      DNS Name of HBase cluster
      For example:
      -c hbasecluster

-p
      Admin password for Ambari of HBase Cluster

-h
      FQDN of headnode0 of HBase cluster
      For example:
      -h hn0-hbclus.dfwefgwwvw.vwegwgwg.cloudapp.net
      (You can find this under the Hosts tab on Ambari UI)

Optional arguments:
-------------------
-u
      Admin user name for Ambari of HBase cluster
      Default = admin

Sample Command:
-----------------
bash hdi_add_zeppelin.sh -c myhbasecluster -p myambaripassword\!123 -h hn0-myhbas.wgfwjgvbeworg.vwoirghrighpw.cloudapp.net





