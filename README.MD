General info about KeyValueLog

KeyValueLog is a Log based key value datastore.

***
*** Basic Entities definition
***

Element: key:value pair, where key is a string and value is a string that either:
1. represents a JSON object, encapsulated by {} and with element delimiter ;
2. represents a comma separated list of values, terminated by \n

Segment: is a file where elements are appended

In-Memory Index: is a hash made of key:offset pairs

***
*** Classes definition
***

The KVLBucket is the class that contains the in-memory index and the segment where Elements are appended 

The interface it provides is made of the following:

write(key,value):
    1 append the new element as key:value at the end of segment file
    2 update in memory index with key:offset

read(key):
    1 get offset for key location in the segment from the in-memory index
    2 if we get a valid offset seek into the segment and return value
    
delete(key):
    logical deletion only, it will place a tombstone in the segment for the key
    0 if key is not inside the in-memory index nothing to do
    1 write a new element appending to segment the couple key:tombstone
    2 updates in-memory index with new offset
    

The Segment is the class that takes care of the operations on the file, at the moment it has two specific implementations for JSON Elements and CSV Elements.

createIndex():
    Starting from an existing Segment file, it produces an index to be stored in memory, useful for initialization and for restarting failed instances

compact():
    To keep segment file size at bay we can compact files, scanning the segment file and keeping only the most recent values for every key. The operation 
    will not modify the existing segment file, but it will create a new one.


    CAVEAT: the segment file may still be too big after clean (too many different keys or too big values)
    PS: the definition of too big may be: "the in-memory index doesn't fit in memory anymore" more likely than "file system quota exceeded"
    return

