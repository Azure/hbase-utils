#! /bin/bash

#----------------------------------------------------------------
# THIS SCRIPT ADDS INSTALLS AND STARTS ZEPPELIN ON HBASE CLUSTER 
#----------------------------------------------------------------


#----------------------------------------------------------------
# PRINT USAGE INFORMATION
#----------------------------------------------------------------


print_usage()
{
cat << ...
Usage:
$0

Mandatory arguments:
--------------------

-p
  Admin password for Ambari
  
-c
  DNS name of HBase Cluster
-h
  FQDN of head node host

...
exit 132
}

#----------------------------------------------------------------
# INITIALIZE PARAMETERS
#----------------------------------------------------------------

AMBARI_PASSWORD=
CLUSTER=
ZEPPELIN_CONFIG='{ "zeppelin.interpreters": "org.apache.zeppelin.jdbc.JDBCInterpreter,org.apache.zeppelin.markdown.Markdown,org.apache.zeppelin.shell.ShellInterpreter", "zeppelin.config.fs.dir": "file:///etc/zeppelin/conf/","zeppelin.anonymous.allowed": "true","zeppelin.notebook.storage": "org.apache.zeppelin.notebook.repo.VFSNotebookRepo", "zeppelin.server.port": "9995", "zeppelin.interpreter.config.upgrade": "true"}'
ZEPPELIN_ENV='{ "log4j_properties_content": "\nlog4j.rootLogger = INFO, dailyfile, ETW, Anonymizer, FullPIILogs\nlog4j.appender.stdout = org.apache.log4j.ConsoleAppender\nlog4j.appender.stdout.layout = org.apache.log4j.PatternLayout\nlog4j.appender.stdout.layout.ConversionPattern=%5p [%d] ({%t} %F[%M]:%L) - %m%n\nlog4j.appender.dailyfile.DatePattern=.yyyy-MM-dd\nlog4j.appender.dailyfile.Threshold = INFO\nlog4j.appender.dailyfile = org.apache.log4j.DailyRollingFileAppender\nlog4j.appender.dailyfile.File = ${zeppelin.log.file}\nlog4j.appender.dailyfile.layout = org.apache.log4j.PatternLayout\nlog4j.appender.dailyfile.layout.ConversionPattern=%5p [%d] ({%t} %F[%M]:%L) - %m%n\n\n#EtwLog Appender\n#sends Phoenix logs to customer storage account\nlog4j.appender.ETW=com.microsoft.log4jappender.EtwAppender\nlog4j.appender.ETW.source=HadoopServiceLog\nlog4j.appender.ETW.component=default\nlog4j.appender.ETW.layout=org.apache.log4j.TTCCLayout\nlog4j.appender.ETW.OSType=Linux\n\n# Anonymize Appender\n# Sends anonymized HDP service logs to our storage account\nlog4j.appender.Anonymizer.patternGroupResource=${patternGroup.filename}\nlog4j.appender.Anonymizer=com.microsoft.log4jappender.AnonymizeLogAppender\nlog4j.appender.Anonymizer.component=default\nlog4j.appender.Anonymizer.layout=org.apache.log4j.TTCCLayout\nlog4j.appender.Anonymizer.Threshold=DEBUG\nlog4j.appender.Anonymizer.logFilterResource=${logFilter.filename}\nlog4j.appender.Anonymizer.source=CentralAnonymizedLogs\nlog4j.appender.Anonymizer.OSType=Linux\n\n# Full PII log Appender\n# Sends  PII HDP service logs to our storage account\nlog4j.appender.FullPIILogs=com.microsoft.log4jappender.FullPIILogAppender\nlog4j.appender.FullPIILogs.component=default\nlog4j.appender.FullPIILogs.layout=org.apache.log4j.TTCCLayout\nlog4j.appender.FullPIILogs.Threshold=DEBUG\nlog4j.appender.FullPIILogs.source=CentralFullServicePIILogs\nlog4j.appender.FullPIILogs.OSType=Linux\nlog4j.appender.FullPIILogs.SuffixHadoopEntryType=true\n","zeppelin_env_content": "# export JAVA_HOME=\nexport JAVA_HOME={{java64_home}}\n# export ZEPPELIN_MEM   # Zeppelin jvm mem options Default -Xms1024m -Xmx1024m -XX:MaxPermSize=512m\n# export ZEPPELIN_INTP_MEM   # zeppelin interpreter process jvm mem options. Default -Xms1024m -Xmx1024m -XX:MaxPermSize=512m\n# export ZEPPELIN_INTP_JAVA_OPTS  # zeppelin interpreter process jvm options.\n# export ZEPPELIN_SSL_PORT  # ssl port (used when ssl environment variable is set to true)\n\n# export ZEPPELIN_LOG_DIR  # Where log files are stored.  PWD by default.\nexport ZEPPELIN_LOG_DIR={{zeppelin_log_dir}}\n# export ZEPPELIN_PID_DIR  # The pid files are stored. ${ZEPPELIN_HOME}/run by default.\nexport ZEPPELIN_PID_DIR={{zeppelin_pid_dir}}\n# export ZEPPELIN_WAR_TEMPDIR # The location of jetty temporary directory.\n# export ZEPPELIN_NOTEBOOK_DIR # Where notebook saved\n# export ZEPPELIN_NOTEBOOK_HOMESCREEN   # Id of notebook to be displayed in homescreen. ex) 2A94M5J1Z\n# export ZEPPELIN_NOTEBOOK_HOMESCREEN_HIDE  # hide homescreen notebook from list when this value set to \"true\". default \"false\"\n# export ZEPPELIN_NOTEBOOK_S3_BUCKET # Bucket where notebook saved\n# export ZEPPELIN_NOTEBOOK_S3_ENDPOINT  # Endpoint of the bucket\n# export ZEPPELIN_NOTEBOOK_S3_USER  # User in bucket where notebook saved. For example bucket/user/notebook/2A94M5J1Z/note.json\n# export ZEPPELIN_IDENT_STRING   # A string representing this instance of zeppelin. $USER by default.\n# export ZEPPELIN_NICENESS   # The scheduling priority for daemons. Defaults to 0.\n# export ZEPPELIN_INTERPRETER_LOCALREPO   # Local repository for interpreters additional dependency loading\n# export ZEPPELIN_NOTEBOOK_STORAGE   # Refers to pluggable notebook storage class, can have two classes simultaneously with a sync between them (e.g. local and remote).\n# export ZEPPELIN_NOTEBOOK_ONE_WAY_SYNC  # If there are multiple notebook storages, should we treat the first one as the only source of truth?\n# export ZEPPELIN_NOTEBOOK_PUBLIC  # Make notebook public by default when created, private otherwise\nexport ZEPPELIN_INTP_CLASSPATH_OVERRIDES=\"{{external_dependency_conf}}\"", "zeppelin_user": "zeppelin", "zeppelin_group": "zeppelin", "zeppelin.spark.jar.dir": "/apps/zeppelin", "zeppelin_log_dir": "/var/log/zeppelin", "zeppelin_pid_dir": "/var/run/zeppelin", "zeppelin.server.kerberos.principal": "", "zeppelin.server.kerberos.keytab": ""}'
ZEPPELIN_SHIRO_INI='{ "shiro_ini_content": "[users]\n# List of users with their password allowed to access Zeppelin.\n# To use a different strategy (LDAP / Database / ...) check the shiro doc at http://shiro.apache.org/configuration.html#Configuration-INISections\nadmin = admin, admin\nuser1 = user1, role1, role2\nuser2 = user2, role3\nuser3 = user3, role2\n\n# Sample LDAP configuration, for user Authentication, currently tested for single Realm\n[main]\n### A sample PAM configuration\n#pamRealm=org.apache.zeppelin.realm.PamRealm\n#pamRealm.service=sshd\n\n\nsessionManager = org.apache.shiro.web.session.mgt.DefaultWebSessionManager\n### If caching of user is required then uncomment below lines\ncacheManager = org.apache.shiro.cache.MemoryConstrainedCacheManager\nsecurityManager.cacheManager = $cacheManager\n\nsecurityManager.sessionManager = $sessionManager\n# 86,400,000 milliseconds = 24 hour\nsecurityManager.sessionManager.globalSessionTimeout = 86400000\nshiro.loginUrl = /api/login\n\n[roles]\nrole1 = *\nrole2 = *\nrole3 = *\nadmin = *\n\n[urls]\n# This section is used for url-based security.\n# You can secure interpreter, configuration and credential information by urls. Comment or uncomment the below urls that you want to hide.\n# anon means the access is anonymous.\n# authc means Form based Auth Security\n# To enfore security, comment the line below and uncomment the next one\n/api/version = anon\n#/api/interpreter/** = authc, roles[admin]\n#/api/configurations/** = authc, roles[admin]\n#/api/credential/** = authc, roles[admin]\n/** = anon\n#/** = authc\n"}'
ZEPPELIN_LOG4J_PROPERTIES='{ "log4j_properties_content": "\nlog4j.rootLogger = INFO, dailyfile, ETW, Anonymizer, FullPIILogs\nlog4j.appender.stdout = org.apache.log4j.ConsoleAppender\nlog4j.appender.stdout.layout = org.apache.log4j.PatternLayout\nlog4j.appender.stdout.layout.ConversionPattern=%5p [%d] ({%t} %F[%M]:%L) - %m%n\nlog4j.appender.dailyfile.DatePattern=.yyyy-MM-dd\nlog4j.appender.dailyfile.Threshold = INFO\nlog4j.appender.dailyfile = org.apache.log4j.DailyRollingFileAppender\nlog4j.appender.dailyfile.File = ${zeppelin.log.file}\nlog4j.appender.dailyfile.layout = org.apache.log4j.PatternLayout\nlog4j.appender.dailyfile.layout.ConversionPattern=%5p [%d] ({%t} %F[%M]:%L) - %m%n\n\n#EtwLog Appender\n#sends Phoenix logs to customer storage account\nlog4j.appender.ETW=com.microsoft.log4jappender.EtwAppender\nlog4j.appender.ETW.source=HadoopServiceLog\nlog4j.appender.ETW.component=default\nlog4j.appender.ETW.layout=org.apache.log4j.TTCCLayout\nlog4j.appender.ETW.OSType=Linux\n\n# Anonymize Appender\n# Sends anonymized HDP service logs to our storage account\nlog4j.appender.Anonymizer.patternGroupResource=${patternGroup.filename}\nlog4j.appender.Anonymizer=com.microsoft.log4jappender.AnonymizeLogAppender\nlog4j.appender.Anonymizer.component=default\nlog4j.appender.Anonymizer.layout=org.apache.log4j.TTCCLayout\nlog4j.appender.Anonymizer.Threshold=DEBUG\nlog4j.appender.Anonymizer.logFilterResource=${logFilter.filename}\nlog4j.appender.Anonymizer.source=CentralAnonymizedLogs\nlog4j.appender.Anonymizer.OSType=Linux\n\n# Full PII log Appender\n# Sends  PII HDP service logs to our storage account\nlog4j.appender.FullPIILogs=com.microsoft.log4jappender.FullPIILogAppender\nlog4j.appender.FullPIILogs.component=default\nlog4j.appender.FullPIILogs.layout=org.apache.log4j.TTCCLayout\nlog4j.appender.FullPIILogs.Threshold=DEBUG\nlog4j.appender.FullPIILogs.source=CentralFullServicePIILogs\nlog4j.appender.FullPIILogs.OSType=Linux\nlog4j.appender.FullPIILogs.SuffixHadoopEntryType=true\n"}'
HEADNODE_HOST_FQDN=
#----------------------------------------------------------------
# PARSE AND PROCESS COMMAND LINE PARAMETERS
#----------------------------------------------------------------
process_arguments() 
{
    while :; do
        case $1 in
            -p|--password)
                if [ -n "$2" ]; then
                    AMBARI_PASSWORD=$2
                    shift
                else
                    printf '[ERROR] -p requires non-empty ambari admin user password.' >&2
                    print_usage
                    exit 1
                fi
                ;;
            -c|--clustername)
                if [ -n "$2" ]; then
                    CLUSTER=$2
                    shift
                else
                    printf '[ERROR] -c requires non-empty DNS name of HBase cluster.' >&2
                    print_usage
                    exit 1
                fi
                ;;
            -h|--hostname)
                if [ -n "$2" ];then
                    HEADNODE_HOST_FQDN=$2
                    shift
                else
                    printf '[ERROR] -h requires non-empty head node host FQDN.' >&2
                    print_usage
                    exit 1
                fi
                ;;
            *)
                break
        esac
        shift
    done
}

#----------------------------------------------------------------
# VALIDATE MANDATORY COMMAND LINE PARAMETERS
#----------------------------------------------------------------

validate_arguments()
{
    if [[ -z "${CLUSTER// }" ]] || [[ -z "${AMBARI_PASSWORD// }" ]] || [[ -z "${HEADNODE_HOST_FQDN// }" ]]
	then
		printf '[ERROR] Mandatory arguments missing.\n' >&2
		print_usage
		exit 1
	fi
}

#----------------------------------------------------------------
# VALIDATE AMBARI CREDENTIALS
#----------------------------------------------------------------

validate_ambari_credentials()
{
    AMBARI_PASSWORD=`echo $AMBARI_PASSWORD`
    
    curl -u admin:$AMBARI_PASSWORD -X GET -H "X-Requested-By: ambari" "https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER?fields=Clusters/desired_configs/hbase-site" -o /tmp/hbase.json 2> /dev/null
	grep -i "access.*denied" /tmp/hbase.json > /dev/null 2>&1

	RESULT=$?
	if [ $RESULT -eq 0 ]
	then
		echo "[ERROR] Invalid Ambari password for cluster $CLUSTER. Exiting!"
		cat /tmp/hbase.json | sed -e 's/^/[INFO] /g'
		exit 134
	else
		echo "[INFO] Cluster credentials successfully validated."
	fi
    
    curl -v -X GET -u admin:$AMBARI_PASSWORD -H "X-Requested-By:ambari" "https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/hosts/$HEADNODE_HOST_FQDN" -o /tmp/hbase.json 2> /dev/null
    grep -i "requested resource doesn't exist" /tmp/hbase.json > /dev/null 2>&1
    
    RESULT=$?
	if [ $RESULT -eq 0 ]
	then
		echo "[ERROR] Host $HEADNODE_HOST_FQDN does not exist for $CLUSTER. Exiting!"
		cat /tmp/hbase.json | sed -e 's/^/[INFO] /g'
		exit 134
	else
		echo "[INFO] Host successfully validated."
	fi
}

#----------------------------------------------------------------
# CREATE CONFIG
#----------------------------------------------------------------

create_config()
{
    curl -u admin:$AMBARI_PASSWORD -i -X POST -d '{"type": "'"$1"'", "tag": "install", "properties": '"$2"'}' https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/configurations -H "X-Requested-By:ambari"
}

#----------------------------------------------------------------
# ADD CONFIG
#----------------------------------------------------------------
apply_config()
{

    curl -u admin:$AMBARI_PASSWORD -i -X PUT -d '{"Clusters": {"desired_configs": { "type": "'"$1"'", "tag" :"install" }}}' https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER -H "X-Requested-By:ambari"
}

#----------------------------------------------------------------
# INSTALL
#----------------------------------------------------------------
install()
{
    curl -u admin:$AMBARI_PASSWORD -i -X PUT -d '{"ServiceInfo": {"state" : "INSTALLED"}}'  https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services/ZEPPELIN -H "X-Requested-By:ambari"
    wait_until "INSTALLED"
}

start_service()
{
    curl -u admin:$AMBARI_PASSWORD -i -X PUT -d '{"ServiceInfo": {"state" : "STARTED"}}'  https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services/ZEPPELIN -H "X-Requested-By:ambari"
    wait_until "STARTED"
}

#----------------------------------------------------------------
# WAIT UNTIL
#----------------------------------------------------------------

wait_until()
{
    finished=0
    while [ $finished -ne 1 ]
    do
      str=$(curl -s -u admin:$AMBARI_PASSWORD https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services/ZEPPELIN)
      if [[ $str == *"$1"* ]] || [[ $str == *"Service not found"* ]] 
      then
        finished=1
      fi
      sleep 3
    done
}
#----------------------------------------------------------------
# MAIN
#----------------------------------------------------------------
process_arguments $@

validate_arguments

validate_ambari_credentials

curl -u admin:$AMBARI_PASSWORD -i -X POST -d '{"ServiceInfo":{"service_name":"ZEPPELIN"}}' https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services -H "X-Requested-By:ambari"

curl -u admin:$AMBARI_PASSWORD -i -X POST -d '' https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/services/ZEPPELIN/components/ZEPPELIN_MASTER -H "X-Requested-By:ambari"
create_config zeppelin-config "$ZEPPELIN_CONFIG"

create_config zeppelin-env "$ZEPPELIN_ENV"

create_config zeppelin-shiro-ini "$ZEPPELIN_SHIRO_INI"

create_config zeppelin-log4j-properties "$ZEPPELIN_LOG4J_PROPERTIES"

apply_config zeppelin-config

apply_config zeppelin-env

apply_config zeppelin-shiro-ini

apply_config zeppelin-log4j-properties

curl -u admin:$AMBARI_PASSWORD -i -X POST -d '{"host_components" : [{"HostRoles":{"component_name":"ZEPPELIN_MASTER"}}] }' https://$CLUSTER.azurehdinsight.net/api/v1/clusters/$CLUSTER/hosts?Hosts/host_name=$HEADNODE_HOST_FQDN -H "X-Requested-By:ambari"

install

start_service
