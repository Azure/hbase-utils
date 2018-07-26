from hdinsight_common.AmbariHelper import AmbariHelper
import requests, json
import logging

UPDATE_DOMAIN = "updateDomain"
RACK = "rack"
FQDN = "fqdn"

def raise_error(msg):
    logger.error(msg)
    raise Exception(msg)

def get_cluster_topology_json(cluster_manifest):
    settings = cluster_manifest.settings
    if "cluster_topology_json_url" in settings:
        json_url = settings["cluster_topology_json_url"]
        r = requests.get(json_url)
        topology_info = r.text
        return topology_info
    else:
        raise_error("Failed to get cluster_topology_json_url from cluster manifest")

def parse_topo_info(cluster_topology_json, fqdn_suffix):
    workernode_info = json.loads(cluster_topology_json)["hostGroups"]["workernode"]
    host_info = []
    for node in workernode_info:
        host = {
                UPDATE_DOMAIN: str(node[UPDATE_DOMAIN]),
                FQDN: node[FQDN]+"."+str(fqdn_suffix),
                RACK: "/rack"+str(node[UPDATE_DOMAIN])
        }
        host_info.append(host)
    return host_info

ambariHelper = AmbariHelper()
cluster_topology_json = get_cluster_topology_json(ambariHelper.get_cluster_manifest())
host_info = parse_topo_info(cluster_topology_json, ambariHelper.get_fqdn().split('.',1)[1])
cluster_name = ambariHelper.cluster_name()
for node in host_info:
    ambariHelper.request_url("clusters/"+cluster_name+"/hosts/"+str(node[FQDN]), "PUT", "{\"Hosts\":{\"rack_info\":\""+str(node[RACK])+"\"}}")
ambariHelper.restart_all_stale_services()

