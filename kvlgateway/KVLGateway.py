import json
import requests

class KVLGateway:
    def __init__(self,registryIp,registryPort):
        self.registryIp = registryIp
        self.registryPort = registryPort

    def routeReadToNode(self,nodeIp,nodePort,key):
        nodeIp = "node1"
        nodePort = "5001"
        nodeEndpoint = "http://" + nodeIp + ":" + nodePort + "/api/v1/elements/?key="+key
        try:
            nodeResponse = requests.get(nodeEndpoint)
            return nodeResponse
        except Exception as err:
            print("An error occurred connecting to Registry" + " > " + str(err))
            return {}
        
