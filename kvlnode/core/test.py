from KVLBucket import KVLBucket
from KVLSegment import KVLSegmentJSON
from KVLSegment import KVLSegmentSimpleValue

import random
import math
import json
import requests


#TEST
testSegment = False 
if testSegment :
    segment = KVLSegmentJSON("example.txt")
    segment.appendKeyValue(2,"{new:try}")
    myoffset = segment.appendKeyValue(1,"{myoffse:tvalue}")
    segment.appendKeyValue(4,"{try:second}")
    retrievedElement = segment.retrieveElement(myoffset)
    print(retrievedElement)

testBucket = False
if testBucket:
    segment = KVLSegmentJSON("example.txt")
    bucket = KVLBucket(segment)
    
    bucket.write("Bobcat","my:value")
    element = bucket.read("Bobcat")
    print(bucket.index)
    print(element)

    bucket.write("Dog","my:fivevalue")
    element = bucket.read("Dog")
    print(bucket.index)
    print(element)
    
    bucket.write("Cat","my:newvalue")
    element = bucket.read("Cat")
    print(bucket.index)
    print(element)

    bucket.write("Snake","my:fivevalue")
    element = bucket.read("Snake")
    print(bucket.index)
    print(element)
    
    bucket.write("Whale","my:fivevalue")
    element = bucket.read("Snake")
    print(bucket.index)
    print(element)
    
    element = bucket.read("Whale")
    print(bucket.index)
    print(element)

    index = bucket.segment.createIndex()
    print (index)

#CreateIndex with random writes and perform Random Reads on new bucket created from file
testCreateIndex = False
if testCreateIndex:
    segment = KVLSegmentJSON("example.txt")
    bucket = KVLBucket(segment)

    foo = ['cat', 'dog', 'mouse', 'duck', 'whale','lion','wolf','pangolin','raccoon']
    
    for i in range (100000):
        key = random.choice(foo)
        value = "population:" + str(random.randint(0,i)*i)
        bucket.write(key,value)
    
    print(bucket.index)
    bucket.segment.flush()

    segment2 = KVLSegmentJSON(segment.filename)
    bucket2 = KVLBucket(segment2)
    index = segment2.createIndex()
    print(index)

    for i in range (10):
        key = random.choice(foo)
        value = bucket2.read(key)
        print (value)

testRandomRead = False
if testRandomRead:
    segment = KVLSegmentJSON("example.txt")
    bucket = KVLBucket(segment)

    foo = ['cat', 'dog', 'mouse', 'duck', 'whale','lion','wolf','pangolin','raccoon']
    
    for i in range (100000):
        key = random.choice(foo)
        value = "population:" + str(random.randint(0,i)*int(math.sqrt(i))) 
        bucket.write(key,value)
    
    for i in range (100):
        key = random.choice(foo)
        value = bucket.read(key)
        print (value)


testGenerateRandomDataset = False
if testGenerateRandomDataset:
    segment = KVLSegmentJSON("segmentfileJSON.txt")
    bucket = KVLBucket(segment)

    foo = ['Mars', 'Venus', 'Pluto', 'Jupiter', 'Saturn','Moon','Earth','Io','Ganimede','Uranus','Neptune','Callisto','Europa']
    bar = ['Explorer','Perseverance', 'Endurance', 'Curiosity', 'Pathfinder', 'Viking', 'Voyager', 'Enterprise', 'Discovery']
    buzz = ['I','II','III','IV','V','VI','VII','VIII','IX','X']
    jsonkey = ['speed','hull','temp']

    for i in range (10000):
        key = random.choice(foo) + " " + random.choice(bar) + " " + random.choice(buzz)
        value = "{\"speed\":\"" + str(random.uniform(0.0,100.0)) + "\","
        value = value + "\"hull\":\"" + str(random.uniform(0.0,100.0)) + "\","
        value = value +"\"temp\":\"" + str(random.uniform(0.0,100.0)) + "\"}"
        bucket.write(key,value)

testRandomDatasetSimpleValue = False
if testRandomDatasetSimpleValue:
    segment = KVLSegmentSimpleValue("segmentfileSV.txt")
    bucket = KVLBucket(segment)

    foo = ['Mars', 'Venus', 'Pluto', 'Jupiter', 'Saturn','Moon','Earth','Io','Ganimede','Uranus','Neptune','Callisto','Europa']
    bar = ['Explorer','Perseverance', 'Endurance', 'Curiosity', 'Pathfinder', 'Viking', 'Voyager', 'Enterprise', 'Discovery']
    buzz = ['I','II','III','IV','V','VI','VII','VIII','IX','X']

    for i in range (10000):
        key = random.choice(foo) + " " + random.choice(bar) + " " + random.choice(buzz)
        value = str(random.uniform(0.0,100.0))
        bucket.write(key,value)

testCompactSegmentJSON = False
if testCompactSegmentJSON:
    segment = KVLSegmentJSON("segmentfileJSON.txt")
    index = segment.shrinkToNewFile()
    print (index)

testCompactSegmentSimpleValue = False
if testCompactSegmentSimpleValue:
    segment = KVLSegmentSimpleValue("segmentfileSV.txt")
    index = segment.shrinkToNewFile()
    print (index)


testInitializeBucketSV = False
if testInitializeBucketSV:
    kvdictionary = {}
    foo = ['Mars', 'Venus', 'Pluto', 'Jupiter', 'Saturn','Moon','Earth','Io','Ganimede','Uranus','Neptune','Callisto','Europa']
    bar = ['Explorer','Perseverance', 'Endurance', 'Curiosity', 'Pathfinder', 'Viking', 'Voyager', 'Enterprise', 'Discovery']
    buzz = ['I','II','III','IV','V','VI','VII','VIII','IX','X']

    for i in range (10000):
        key = random.choice(foo) + " " + random.choice(bar) + " " + random.choice(buzz)
        value = str(random.uniform(0.0,100.0))
        kvdictionary[key] = value

    segment = KVLSegmentSimpleValue('testinit.txt')
    bucket = KVLBucket(segment)

    bucket.initializeBucket(kvdictionary)

    print(bucket.index)

testInitializeBucketJSON = False
if testInitializeBucketJSON:
    kvdictionary = {}
    foo = ['Mars', 'Venus', 'Pluto', 'Jupiter', 'Saturn','Moon','Earth','Io','Ganimede','Uranus','Neptune','Callisto','Europa']
    bar = ['Explorer','Perseverance', 'Endurance', 'Curiosity', 'Pathfinder', 'Viking', 'Voyager', 'Enterprise', 'Discovery']
    buzz = ['I','II','III','IV','V','VI','VII','VIII','IX','X']

    for i in range (10000):
        key = random.choice(foo) + " " + random.choice(bar) + " " + random.choice(buzz)
        value = "{\"speed\":\"" + str(random.uniform(0.0,100.0)) + "\","
        value = value + "\"hull\":\"" + str(random.uniform(0.0,100.0)) + "\","
        value = value +"\"temp\":\"" + str(random.uniform(0.0,100.0)) + "\"}"
        kvdictionary[key] = value

    segment = KVLSegmentJSON('testinit.txt')
    bucket = KVLBucket(segment)

    bucket.initializeBucket(kvdictionary)

    print(bucket.index)

testAPI = False
if testAPI:
    
    url = 'http://localhost:5001/api/v1/elements/'
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
