import json
import requests
import hashlib
from datetime import datetime

class KVLGateway:
    def __init__(self,registryIp,registryPort):
        self.registryIp = str(registryIp)
        self.registryPort = str(registryPort)
        self.nodes = {}
        self.hnodeorderedlist = []

    def routeReadToNode(self,nodeIp,nodePort,key):
        nodeEndpoint = "http://" + nodeIp + ":" + nodePort + "/api/v1/elements/?key="+key
        try:
            nodeResponse = requests.get(nodeEndpoint)
            return nodeResponse
        except Exception as err:
            print("An error occurred connecting to Registry" + " > " + str(err))
            return {}
        
    def hashAsInt(self,value):
        h = hashlib.md5(value.encode())
        hdigest =  h.hexdigest()
        return int(hdigest,16)
 

    def onBoardingNode(self,nodeIp,nodePort):
        now = datetime.now()
        timestring = now.strftime('%f')
        key = self.hashAsInt(nodeIp+nodePort+timestring)
        if key not in self.nodes.keys():
            #node passed straight as a dictionary, maybe some sort of struct could be a better solution TODO
            self.nodes[key] = {'ip':nodeIp,'port':nodePort, 'added':timestring}
            if len(self.hnodeorderedlist) == 0 or self.hnodeorderedlist[-1] < key:
                self.hnodeorderedlist.append(key)
            else: 
                # more efficient then just appending key to hnodeorderedlist and 
                # then sorting it: O(n) vs O(nlogn). This could be done even better
                # (O(logn)) recursively comparing key with the medium element of the 
                # array and moving to the right half (k is greater than pivot) or the
                # left one.                  
                for i in range(len(self.hnodeorderedlist)):
                    if self.hnodeorderedlist[i] > key:
                        firsthalf = self.hnodeorderedlist[:i]
                        secondhalf = self.hnodeorderedlist[i:]
                        firsthalf.append(key)
                        self.hnodeorderedlist = firsthalf + secondhalf
                        break    
            return True
        else:
            return False

    def routeKeyToNode(self,key):
        hkey = self.hashAsInt(key)
        for hvalue in self.hnodeorderedlist:
            if hkey < hvalue:
                return hvalue
        # last portion of the ring is managed by the same node that manages the first portion,
        # so keys that go beyond the last hashed node value are kept by the first node (ordered 
        # by hashvalue)  
        return self.hnodeorderedlist[-1]


