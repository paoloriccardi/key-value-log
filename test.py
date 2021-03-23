from KVLSegment import KVLSegmentJSON
from KVLBucket import KVLBucket
from KVLJanitor import KVLJanitor

import random
import math


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

testJanitorCompact = True
if testJanitorCompact:
    segment = KVLSegmentJSON("example.txt")
    bucket = KVLBucket(segment)
    janitor = KVLJanitor()

    foo = ['cat', 'dog', 'mouse', 'duck', 'whale','lion','wolf','pangolin','raccoon']
    
    for i in range (100000):
        key = random.choice(foo)
        value = "population:" + str(random.randint(0,i)*i)
        bucket.write(key,value)
    
    print(bucket.index)

    segment2 = janitor.compactSegmentJSON(bucket.segment)
    bucket2 = KVLBucket(segment2)
    index = segment2.createIndex()
    print(index)

    for i in range (10):
        key = random.choice(foo)
        value = bucket2.read(key)
        print (value)
