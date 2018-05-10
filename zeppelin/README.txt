INTRODUCTION
--------------

This script adds Zeppelin notebook service to HDInsight HBase Clusters which do not already have it in easiest possible way so that the JDBC Phoenix interpreter is set up and ready to use.

Details on how to install and use the hdi_add_zeppelin.sh are provided below.

INSTALLATION
--------------

Download hdi_add_zeppelin.sh on the head node of your HDI HBase cluster.

USAGE
-------------
sudo -E bash hdi_add_zeppelin.sh

**** IF EXECUTED SUCCESSFULLY YOU SHOULD BE ABLE TO SEE ZEPPELIN UNDER AMBARI DASHBOARD, ACCESS ZEPPELIN UI, CREATE NOTEBOOKS AND USE %jdbc(phoenix) INTERPRETER ****



