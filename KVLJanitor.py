from KVLBucket import KVLBucket
from KVLSegment import KVLSegmentJSON

from datetime import datetime
import sys

class KVLJanitor():
    def __init__(self):
        pass

    def compactSegmentJSON(self, segment):
        index = segment.createIndex()
        now = datetime.now()
        prefix = now.strftime('%f')
        newFilename = prefix + segment.filename
        newSegment = KVLSegmentJSON(newFilename)

        for key,offset in index.items():
            value = segment.retrieveValue(offset) 
            newSegment.appendKeyValue(key,value)

        return newSegment

    def createBucket(self, filename):
        segment = KVLSegmentJSON(filename)
        bucket = KVLBucket(segment) 
        return bucket

