import datetime
import requests
import json

class KVLRegistry:
    def __init__(self):
        #array of KVLRegistryEntry
        self.registry = []

    def register(self,regEntry):
        #Nodes can register to the registry (they will try to register at startup)
        if regEntry.alreadyRegistered():
            return False
        if regEntry.check():
            self.registry.append(regEntry)
            return True
        return False

    def alreadyRegistered(self,entry):
        for i in range(self.registry):
            curEntry = self.registry[i]
            if curEntry.equalsTo(entry):
                return True
        return False

    def unregister(self,unregEntry):
        #Nodes can unregister from the registry
        for i in range(self.registry):
            entry = self.registry[i]
            if entry.equalsTo(unregEntry):
                self.registry.remove(i)
                return True
        return False

    def healthcheck(self):
        #scans node list to see if they are alive
        tempregistry = self.registry
        for i in range(self.registry):
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
        try:
            nodeResponse = requests.get(nodeEndpoint)
        except Exception as err:
            print("An error occurred healthcheking node " + self.ip + ":" + self.port + " > " + str(err))
            return False

        if nodeResponse.status_code == 200:
            return True
        else:
            return False
    
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




