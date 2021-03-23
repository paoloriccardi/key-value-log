import sys

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
        #2 if we get a valid offset, seek into the segment and return value
        elementOffset = self.index[str(key)]
        element = self.segment.retrieveElement(elementOffset)
        return element

    def delete(self,key):
        #logical deletion only, it will place a tombstone in the segment for the key
        #0 if key is not inside the in-memory index nothing to do
        #1 write a new element appending to segment the couple key:tombstone (key:{})
        #2 updates in-memory index with new offset
        tombstone = self.segment.getTombstoneValue()
        self.write(key,tombstone)
        return  


#Segment with JSON as values delimited by {}
class KVLSegmentJSON():
    def __init__(self,filename):
        try:
            self.file = open(filename,"a+")
        except OSError:
            print ("Could not open file" + filename + "\n")
            sys.exit()
    
    def appendKeyValue(self, key, value):
        elementString = str(key) + ":{" + str(value) + "};"
        rwpointer = self.file.tell()
        if rwpointer > 1:
            #takes into account element separator ;
            rwpointer = rwpointer 
        try:
            self.file.write(elementString)
        except OSError:
            print ("Could not append Key:Value \n")
            return -1
        return rwpointer
    
    def retrieveElement(self,offset):
        self.file.seek(offset)
        elementString = ""
        while True:
            character = self.file.read(1)
            if not character:
                break
            if character == "}":
                elementString = elementString + character
                break
            elementString = elementString + character
        return elementString
    
    def createIndex(self):
        self.file.seek(0)
        index = {}
        prevchar = self.file.read(1) 
        if not prevchar:
            return index
        #non trivial case
        self.file.seek(0)
        key = ""
        scanningKey = True
        prevchar = ""
        rwindex = 0
        offset = 0
        for char in self.file.read():
            if scanningKey and char == ":":
                scanningKey = False
                index[key]=offset 
                key = ""
            elif scanningKey and char != ":":
                key = key + char
            elif not scanningKey and prevchar == "}" and char == ";":
                scanningKey = True
                offset = rwindex +1
            prevchar = char
            rwindex = rwindex + 1
        return index
    
    def flush(self):
        #close segment file
        self.file.close()

    def compact(self):
        pass

    def getTombstoneValue(self):
        return "{}"

#Segment with Lines and \n as element delimiter
class KVLSegmentLines():
    def __init__(self,filename):
        try:
            self.file = open(filename,"a+")
        except OSError:
            print ("Could not open file" + filename + "\n")
            sys.exit()
    
    def appendKeyValue(self, key, value):
        #\n cannot appear inside either key or value
        elementString = str(key) + ":" + value + "\n"
        rwpointer = self.file.tell()
        try:
            self.file.write(elementString)
        except OSError:
            print ("Could not append Key:Value \n")
            return -1
        return rwpointer

    def retrieveElement(self,offset):
        self.file.seek(offset)
        elementString = self.file.readline()
        return elementString
    
    def createIndex(self):
        index = {}
        if not self.file.read(1):
            return index
        #non trivial case, TODO
        return

    def compact(self):
        pass

    def getTombstoneValue(self):
        return ""

