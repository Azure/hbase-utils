from hdinsight_common.AmbariHelper import AmbariHelper
import requests, json, re
import subprocess, sys
import pty

if(len(sys.argv) < 2):
    print "Error: Usage: restartdeadregionserverswithlocalhostissue.py <ssh-username>"
    sys.exit(1)

a=AmbariHelper()
result=a.query_url("clusters/"+a.cluster_name()+"/hosts")

for i in result['items']:
    if(i['Hosts']['host_name'].startswith("zk")):
        result_hbase_metrics=a.query_url("clusters/"+a.cluster_name()+"/hosts/"+i['Hosts']['host_name']+"/host_components/HBASE_MASTER")
        if(result_hbase_metrics['metrics']['hbase']['master']['IsActiveMaster'] == 'true'):
            url="http://"+i['Hosts']['host_name']+":16010/jmx?qry=Hadoop:service=HBase,name=Master,sub=Server"
            req=requests.get(url)
            res=req.json()
            deadregionservers = res['beans'][0]['tag.deadRegionServers'].split(';')

BASH_PATH="/bin/bash"
HBASE_DAEMON_PATH="/usr/hdp/current/hbase-regionserver/bin/hbase-daemon.sh"
HBASE_CONFIG_PATH="/usr/hdp/current/hbase-regionserver/conf"

COMMANDLOGTAIL="sudo tail -n100 /var/log/hbase/hbase-hbase-regionserver-wn*.log"
COMMANDSTOP="sudo -u hbase " + BASH_PATH + " " + HBASE_DAEMON_PATH + " --config " + HBASE_CONFIG_PATH + " stop regionserver"
COMMANDSTART="sudo -u hbase " + BASH_PATH + " " + HBASE_DAEMON_PATH + " --config " + HBASE_CONFIG_PATH + " start regionserver"
LOCALHOSTISSUESTRING = 'WARN  \[regionserver/localhost/127.0.0.1:16020\] regionserver.HRegionServer: error telling master we are up'
SSHUSER = sys.argv[1] 
for deadregionserver in filter(None,deadregionservers):
    print "Processing dead region server " + deadregionserver
    HOST=SSHUSER+"@"+deadregionserver.split(',')[0]
    ssh=subprocess.Popen(["ssh", "%s" % HOST, COMMANDLOGTAIL],shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result=ssh.stdout.readlines()
    if result == []:
        error = ssh.stderr.readlines()
        print >>sys.stderr, "ERROR: %s" % error
        sys.exit(1)
    else:
        if re.search(LOCALHOSTISSUESTRING, str(result)) is None:
            print "Region server " + deadregionserver + " does not show localhost issue; proceeding to next in the list"
            continue
        else:
            print "localhost issue found, proceeding with RS restart attempt"

    for COMMAND in (COMMANDSTOP, COMMANDSTART):
        print "Now executing " + COMMAND
        ssh=subprocess.Popen(["ssh", "%s" % HOST, COMMAND],shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result=ssh.stdout.readlines()
        print result
        if result == []:
            error = ssh.stderr.readlines()
            print >>sys.stderr, "ERROR: %s" % error
            sys.exit(1)
        else:
            print "Execution of command " + COMMAND + " successful"
print "Finished processing all dead region servers"    
