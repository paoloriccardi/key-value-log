import datetime
import requests
import json
import time

class KVLRegistry:
    def __init__(self):
        #array of KVLRegistryEntry
        self.registry = []

    def register(self,regEntry):
        #Nodes can register to the registry (they will try to register at startup)
        if self.alreadyRegistered(regEntry):
            return False
        self.registry.append(regEntry)
        
        gwIp = "gateway"
        gwPort = "7001"

        nodeIp = regEntry.ip
        nodePort = regEntry.port

        nodeEndpoint = "http://" + gwIp + ":" + gwPort + "/api/v1/conf/onboard/"
        head = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}
        jsondata = json.dumps({'ip':nodeIp,'port':nodePort}) 

        try:
            nodeResponse = requests.post(nodeEndpoint,data=jsondata,headers=head)
        except Exception as err:
            print("An error occurred connecting to Registry" + " > " + str(err))
            return False
        if nodeResponse.status_code == 200:
            return True
        else:
            return False
        
    def alreadyRegistered(self,entry):
        for i in range(len(self.registry)):
            curEntry = self.registry[i]
            if curEntry.equalsTo(entry):
                return True
        return False

    def getRegistered(self,entry):
        for curEntry in self.registry:
            if curEntry.equalsTo(entry):
                return curEntry
        return

    def unregister(self,unregEntry):
        #Nodes can unregister from the registry
        for i in range(len(self.registry)):
            entry = self.registry[i]
            if entry.equalsTo(unregEntry):
                self.registry.remove(i)
                return True
        return False

    def healthcheck(self):
        #scans node list to see if they are alive
        tempregistry = self.registry
        for i in range(len(self.registry)):
            entry = tempregistry[i]
            if not entry.check():
                tempregistry.remove(i)
        self.registry = tempregistry
        return


class KVLRegistryEntry:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.created = datetime.datetime.now()

    def isExpired(self):
        leasetime = datetime.timedelta(minutes=10)
        now = datetime.datetime.now()
        if self.created + leasetime < now:
            return True
        return False

    def renewLease(self):
        self.created = datetime.datetime.now()
        return 

    def check(self):
        #check whether the Entry is alive or not
        nodeEndpoint = "http://" + self.ip + ":" + self.port + "/api/v1/internals/heartbeat/"
        result = False
        for _ in range(6):
            try:
                nodeResponse = requests.get(nodeEndpoint)
            except Exception as err:
                print("An error occurred healthcheking node " + self.ip + ":" + self.port + " > " + str(err))

            if nodeResponse.status_code == 200:
                result = True
                break
            time.sleep(10)
        return result
    
    def equalsTo(self,comparedEntry):
        if self.ip != comparedEntry.ip or self.port != comparedEntry.port:
            return False
        return True

    def toJSON(self):
        dictionary = {}
        dictionary['ip'] = self.ip
        dictionary['port'] = self.port
        dictionary['created'] = self.created
        return dictionary




