import random
import requests
import json
from KVLRegistry import KVLRegistryEntry


testCheckNodeAlive = False
if testCheckNodeAlive:

    #wrong port provided
    entry = KVLRegistryEntry('localhost','5002')
    print(entry.toJSON())
    if entry.check():
        print ("Node " + entry.ip + ":"+ entry.port + " is Alive")
    else:
        print ("Node " + entry.ip + ":"+ entry.port + " is Dead")

    #right port:ip provided
    entry = KVLRegistryEntry('localhost','5001')
    print(entry.toJSON())
    if entry.check():
        print ("Node " + entry.ip + ":"+ entry.port + " is Alive")
    else:
        print ("Node " + entry.ip + ":"+ entry.port + " is Dead")

testAPI = False
if testAPI:
    
    url = 'http://localhost:7001/api/v1/elements/'
    headers = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}
    
    kvdictionary = {}
    foo = ['Mars', 'Venus', 'Pluto', 'Jupiter', 'Saturn','Moon','Earth','Io','Ganimede','Uranus','Neptune','Callisto','Europa']
    bar = ['Explorer','Perseverance', 'Endurance', 'Curiosity', 'Pathfinder', 'Viking', 'Voyager', 'Enterprise', 'Discovery']
    buzz = ['I','II','III','IV','V','VI','VII','VIII','IX','X']

    for i in range (100):
        key = random.choice(foo) + " " + random.choice(bar) + " " + random.choice(buzz)
        value = str(random.uniform(0.0,100.0))
        kvdictionary[key] = value

    for key,value in kvdictionary.items():
        jsondata = json.dumps({'key':key,'value':value})   
        r = requests.post(url, data=jsondata, headers=headers)


    