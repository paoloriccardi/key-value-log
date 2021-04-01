from datetime import datetime
import random

class KVLBucket:
    def __init__(self,segment):
        self.segment = segment
        self.index = self.segment.createIndex()

    def write(self,key,value):
        #1 append the new element as key:value at the end of segment file
        #2 update in memory index with key:offset
        elementOffset = self.segment.appendKeyValue(key,value)
        if elementOffset < 0:
            print ("some error prevented appending to file")
            return
        self.index[str(key)] = elementOffset
        return 

    def read(self,key):
        #1 get offset for key location in the segment from the in-memory index
        #2 if we get a valid offset, seek into the segment and return [key,value]
        if str(key) not in self.index:
            return 
        elementOffset = self.index[str(key)]
        element = self.segment.retrieveValue(elementOffset)
        return [key,element]

    def delete(self,key):
        #logical deletion only, it will place a tombstone in the segment for the key
        #0 if key is not inside the in-memory index nothing to do
        #1 write a new element appending to segment the couple key:tombstone (key:{})
        #2 updates in-memory index with new offset
        if key not in self.index.keys():
            return
        tombstone = self.segment.getTombstoneValue()
        self.write(key,tombstone)
        return 

    def reload(self,newSegment):
        self.segment.flush()
        self.segment = newSegment
        self.index = self.segment.createIndex()
        return

    def compact(self):
        # asks to his segment to compact
        # segment.compactSelf returns the index of the new file used by segment 
        # old index is overwritten by the new one
        # old segment file is not changed nor modified but persisted on disk
        
        newIndex = self.segment.shrinkToNewFile()
        
        #should verify old index against new index before return
        self.index = newIndex
        return True

    def initializeBucket(self,kvdict):
        index = self.segment.initializeSegment(kvdict)
        self.index = index
        return