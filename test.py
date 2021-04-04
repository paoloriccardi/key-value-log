import json
import requests
import random

#Initialize the Bucket Nodes with some key:values
testAPI = True
if testAPI:
    
    url = 'http://localhost:7001/api/v1/elements/'
    headers = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}
    
    kvdictionary = {}
    foo = ['Mars', 'Venus', 'Pluto', 'Jupiter', 'Saturn','Moon','Io','Ganimede','Uranus','Neptune','Callisto','Europa','Phobos','Deimos','Titan','Mercury','Triton','Eris','Titania','Oberon','Rhea',]
    bar = ['Explorer','Perseverance', 'Endurance', 'Curiosity', 'Pathfinder', 'Viking', 'Voyager', 'Enterprise', 'Discovery', 'Beagle','Phoenix','Lander','Ingenuity','Elite']
    buzz = ['I','II','III','IV','V','VI','VII','VIII','IX','X','XI','XII','XIII','XIV','XV']

    for i in range (10000):
        key = random.choice(foo) + " " + random.choice(bar) + " " + random.choice(buzz)
        value = str(random.uniform(0.0,100.0))
        kvdictionary[key] = value

    for key,value in kvdictionary.items():
        jsondata = json.dumps({'key':key,'value':value})   
        r = requests.post(url, data=jsondata, headers=headers)