import sys

import json

from datetime import datetime

#Segment with JSON as values delimited by {}
class KVLSegmentJSON():
    def __init__(self,filename):
        self.homedir = "files/"
        self.filename =  filename
        self.filepath = self.homedir + self.filename
        try:
            self.file = open(self.filepath,"a+")
        except OSError:
            print ("Could not open file" + self.filepath + "\n")
            sys.exit()
    
    def appendKeyValue(self, key, value):
        if not self.checkValueFormat(value):
            print (value)
            print ("json format error in input")
            exit()
        #stringValue = json.dumps(value)
        elementString = str(key) + ":" + value + ";"
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
    
    def checkValueFormat(self,value):
        try:
            json.loads(value)
        except ValueError as e:
            print("Error in JSON format value " + str(e))
            return False
        return True

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
                valueString = character
                scanningValue = True
            elif not scanningValue and character != "{":
                pass
            elif scanningValue and character == "}":
                valueString = valueString + character
                break
            else:
                valueString = valueString + character
        return valueString
    
    def createIndex(self):
        #scan one char at the time the segment file from beginning to end which is very inefficient
        #this method should be used only when a consistent/updated version of index is not available (e.g. from an existing bucket)
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
         
    def getTombstoneValue(self):
        return "{}"
 
    def flush(self):
        #close segment file
        self.file.close()       
 
    def attachNewFile(self,newFilename):
        self.filename = newFilename
        try:
            self.filepath = self.homedir + self.filename
            self.file = open(self.filepath, "a+")
        except OSError:
            print ("Could not open file" + newFilename + "\n")
            sys.exit()
        
    def compactSelf(self):
        now = datetime.now()
        prefix = now.strftime('%f')
        newFilename = prefix + self.filename 

        inmemoryKV = self.inMemoryKeyValue()
        self.flush()
        self.attachNewFile(newFilename)

        for key,value in inmemoryKV.items(): 
            self.appendKeyValue(key,value)

        newIndex = self.createIndex()
        return newIndex

    def inMemoryKeyValue(self):
        KVDict = {}
        index = self.createIndex()
        for key,offset in index.items():
            value = self.retrieveValue(offset) 
            KVDict[key]=value
        return KVDict

#Segment with value equal to simple value, key:values separated by ;
class KVLSegmentSimpleValue():
    def __init__(self,filename):
        self.homedir = "files/"
        self.filename =  filename
        self.filepath = self.homedir + self.filename
        try:
            self.file = open(self.filepath,"a+")
        except OSError:
            print ("Could not open file" + self.filepath + "\n")
            sys.exit()
    
    def appendKeyValue(self, key, value):
        elementString = str(key) + ":" + str(value) + ";"
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
            if character == ";":
                elementString = elementString
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
            if not scanningValue and character == ":":
                scanningValue = True
            elif not scanningValue and character != ":":
                pass
            elif scanningValue and character == ";":
                valueString = valueString 
                break
            else:
                valueString = valueString + character
        return valueString
    
    def createIndex(self):
        #scan one char at the time the segment file from beginning to end which is very inefficient
        #this method should be used only when a consistent/updated version of index is not available (e.g. from an existing bucket)
        self.file.seek(0)
        index = {}
        if not self.file.read(1):
            return index
        #non trivial case
        self.file.seek(0)
        key = ""
        scanningKey = True
        rwindex = 0
        offset = 0
        for char in self.file.read():
            if scanningKey and char == ":":
                scanningKey = False
                index[key]=offset 
                key = ""
            elif scanningKey and char != ":":
                key = key + char
            elif not scanningKey and char != ";":
                pass
            elif not scanningKey and char == ";":
                scanningKey = True
                offset = rwindex +1
            rwindex = rwindex + 1
        return index
    
    def getTombstoneValue(self):
        return ""

    def checkValueFormat(self,value):
        return True

    def flush(self):
        #close segment file
        self.file.close()       
    
    def attachNewFile(self,newFilename):
        self.filename = newFilename
        try:
            self.filepath = self.homedir + self.filename
            self.file = open(self.filepath, "a+")
        except OSError:
            print ("Could not open file" + newFilename + "\n")
            sys.exit()
        
    def compactSelf(self):
        now = datetime.now()
        prefix = now.strftime('%f')
        newFilename = prefix + self.filename
        

        inmemoryKV = self.inMemoryKeyValue()
        self.flush()
        self.attachNewFile(newFilename)

        for key,value in inmemoryKV.items(): 
            self.appendKeyValue(key,value)

        newIndex = self.createIndex()
        return newIndex

    def inMemoryKeyValue(self):
        KVDict = {}
        index = self.createIndex()
        for key,offset in index.items():
            value = self.retrieveValue(offset) 
            KVDict[key]=value
        return KVDict