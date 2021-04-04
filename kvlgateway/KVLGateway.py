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
            return nodeResponse.json()
        except Exception as err:
            print("An error occurred connecting to Registry" + " > " + str(err))
            return {}
        
    def routeWriteToNode(self,nodeIp,nodePort,kvdict):
        ip= nodeIp
        port = nodePort

        nodeEndpoint = "http://" + ip + ":" + port + "/api/v1/elements/"
        head = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}
        key = kvdict['key']
        value = kvdict['value']
        jsondata = json.dumps({'key':key,'value':value}) 

        try:
            nodeResponse = requests.post(nodeEndpoint,data=jsondata,headers=head)
            return nodeResponse.json()
        except Exception as err:
            print("An error occurred routing request to Node" + " > " + str(err))
            return {}

    def hashAsInt(self,value):
        # one should consider how evenly the python md5 algo distributes his output
        h = hashlib.md5(value.encode())
        hdigest =  h.hexdigest()[:8]
        return int(hdigest,16)
 
    def gatherAllKeys(self):
        listofkeys = []
        for node in self.nodes.values():
            nodekeys = self.getKeysFromNode(node['ip'],node['port'])
            listofkeys = listofkeys + nodekeys
        return listofkeys

    def getKeysFromNode(self,nodeIp,nodePort):
        nodeEndpoint = "http://" + nodeIp + ":" + nodePort + "/api/v1/elements/all/"
        try:
            nodeResponse = requests.get(nodeEndpoint)
            listofkeys = []
            for key in nodeResponse.json():
                listofkeys.append(key)
            return listofkeys
        except Exception as err:
            print("An error occurred connecting to Registry" + " > " + str(err))
            return []

  
    def getKeysBelowHashInNode(self,node,newhash):
        # returns a list with the keys k of node such as hash(k) < newhash
        # newhash is the hash of a new node, so we are interested
        # in determining the keys that will move from the old node to
        # the new one
        listKeys = self.getKeysFromNode(node['ip'],node['port'])
        if listKeys == []:
            return listKeys
        outList = []
        for i in range(len(listKeys)):
            key = listKeys[i]
            if self.hashAsInt(key) < newhash:
                outList.append(key)
        return outList
    
    def moveKey(self,key,node_origin,node_destination):
        # read key from node_origin and write it to node_destination
        # useful to migrate groups of keys from an old node to a new one
        readResponse = self.routeReadToNode(node_origin['ip'],node_origin['port'],key)
        if readResponse != {}:
            writeResponse = self.routeWriteToNode(node_destination['ip'],node_destination['port'],readResponse)
            if writeResponse != {}:
                return True
        return False 
      


    def onBoardingNode(self,nodeIp,nodePort):
        now = datetime.now()
        timestring = now.strftime('%f')
        hashnode = self.hashAsInt(nodeIp+nodePort+timestring)

        if len(self.hnodeorderedlist) == 0:
            self.hnodeorderedlist.append(hashnode)
            self.nodes[hashnode] = {'ip':nodeIp,'port':nodePort, 'added':timestring}
            return True

        if hashnode not in self.nodes.keys():
            # since the hash is calculated on ip+port+nonce 
            # the same node (ip:port) could be added more than 
            # once to the list
            ordered_hash = self.hnodeorderedlist
            #find hashnode location in the sorted hash(node) list
            for i in range(len(ordered_hash)):
                if ordered_hash[i] > hashnode:
                    firsthalf = ordered_hash[:i]
                    secondhalf = ordered_hash[i:]
                    firsthalf.append(hashnode)
                    ordered_hash = firsthalf + secondhalf
                    if i ==len(ordered_hash) -1:
                        impacted_node_hash = ordered_hash[0]
                    else:
                        impacted_node_hash = secondhalf[0]
                    break
                elif ordered_hash[i] <= hashnode and i == len(ordered_hash) - 1:
                    impacted_node_hash = ordered_hash[0]
                    ordered_hash.append(hashnode)
                    break
            
            origin = self.nodes[impacted_node_hash]
            destination = {'ip':nodeIp,'port':nodePort, 'added':timestring}

            keysToMigrate = self.getKeysBelowHashInNode(origin,hashnode)

            for key in keysToMigrate:
                self.moveKey(key,origin,destination)
            #new node is ready now

            self.hnodeorderedlist = ordered_hash
            #node passed straight as a dictionary (maybe a named tuple could be better)
            self.nodes[hashnode] = {'ip':nodeIp,'port':nodePort, 'added':timestring}
            
            return True
        else:
            return False

    def resolveKeyToNode(self,key):
        hkey = self.hashAsInt(key)
        for hvalue in self.hnodeorderedlist:
            if hkey < hvalue:
                return hvalue
        # last portion of the ring is managed by the same node that manages the first portion,
        # so keys that go beyond the last hashed node value are kept by the first node (ordered 
        # by hashvalue)  
        return self.hnodeorderedlist[0]


