#Interface definition

#<Key:Value> related operations

def write(key,value):
    #1 append the new element as key:value at the end of segment file
    #2 update in memory index with key:offset
    return

def readValue(key):
    #1 get offset for key location in the segment from the in-memory index
    #2 if we get a valid offset seek into the segment and return value
    return

def deleteKey(key):
    #logical deletion only, it will place a tombstone in the segment for the key
    #0 if key is not inside the in-memory index nothing to do
    #1 write a new element appending to segment the couple key:tombstone
    #2 updates in-memory index with new offset
    return

def findKey(key):
    #1 if key is not present inside in-memory index returns -1 
    #2 else returns the offset for key
    return  

#Segment related operations

def clean():
    # when a segment file become too big because of duplicate keys
    # we will be interested in keeping only the last version of every key in
    # the working segment file
    #1 compact the working segment file, two alternative approaches:
    #   1.a using the actual in memory index get every key:value in the index and append it to a new segment file on disk
    #   1.b scan backwards the wsf and append to the new segment file on disk only the most recent key:value for every key
    #2 generate new in memory index (e.g. scanning new segment file)
    #3 substitute old in-memory index with new one
    #4 archive old segment file

    #CAVEAT: the segment file may still be too big after clean (too many different keys or too big values)
    #PS: the definition of too big may be: "the in-memory index doesn't fit in memory anymore" more likely than "file system quota exceeded"
    return

#Entities definition

#Element: key:value pair, where key is an integer and value is a string (what about a JSON object?)
#Segment: is a file where elements are appended (with or without separators? what about binary format?)
#In-Memory Index: is a hash made of key:offset pairs (how big can it grow?)
