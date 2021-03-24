import sys
from os import getcwd

from datetime import datetime

#Segment with JSON as values delimited by {}
class KVLSegmentJSON():
    def __init__(self,filename):
        self.filename =  filename
        try:
            self.file = open(self.filename,"a+")
        except OSError:
            print ("Could not open file" + self.filename + "\n")
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

    def retrieveValue(self,offset):
        self.file.seek(offset)
        valueString = ""
        scanningValue = False
        while True:
            character = self.file.read(1)
            if not character:
                break
            if not scanningValue and character == "{":
                scanningValue = True
            elif not scanningValue and character != "{":
                pass
            elif scanningValue and character == "}":
                valueString = valueString 
                break
            else:
                valueString = valueString + character
        return valueString
    
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

