# General info about KeyValueLog

KeyValueLog is a Log based key:value datastore.
It was made for fun and educational purposes only.

## Overall
New key:value tuples or new values for keys are just appended to a text file called Segment.
An in memory index of where the last updated value is located for each key is kept and updated for faster reads. 
The index is a dictionary made of key:offset pairs, the offset represents the position where the last write of the key is located inside the Segment file.

### Basic assumptions and ideas 
1. No key:value pair can be deleted once it has been inserted
2. Pairs are persisted appending them at the end of a textfile 
3. To help reads an in-memory Index is mantained
4. We assume that we need to read just the most recent key:value pair

## File management
Despite the fact that a read operation returns just the last updated key:value pair, no key:value pair is e ever deleted from Segment files.
The active Segment file can be compacted for practical reason when it reaches a certain size limit. 
When the above conditions are met, a Bucket may ask to his Segment to compact itself and it will generate his new in-memory index right afterward on the new, compacted, Segment. 
In order to generate a new shrinked file, the Segment class will scan the actual segment file and will generate a temporary dictionary of key:value pairs. 
The old file will be detached from the Segment and the new file will be attached to the Segment class. 
The older Segment files will be persisted and never touched or midified again.

## Classes entities and modules
A Bucket is the main component of the system. It has a Segment and keeps the in-memory Index for the segment contents. 
The Bucket interface provides the following:
1. write -> append the pair to the Segment file and updates the in-memory Index
2. read -> returns the pair key,element retrieved directly from the Segment file, using in-memory Index offset for key
3. delete -> writes key:tombstone, at the moment this operation is purely logical, no physical deletion intended
4. compact -> generate a new segment file for the current segment and a new in-memory Index
5. reload -> allows to reaload a new segment instead of the old one, re-initializing the index file and detaching the old segment file from the old segment

A Segment manages the Segment file and every operation related to it. A segment provides an interface which allows the following:
1. Append/retrieve of elements to/from the Segment file
2. Formatting functionalities which in some sense requires for each Element type to have its own specific Segment concrete implementations (which is not optimal)
3. Building or re-building the index dictionary from an existing Segment file, very useful for reinitiate a new instance, crash recovery or Segment file compactions (see Janitor)
4. Specific tombstone element for each Element type 
5. Facilities to close the current Segment file reference and very basic IO error handling 
6. Compaction utilities

An Element: key:value pair, where key is a string and value is a string that either:
1. represents a JSON object, encapsulated by {} and with element delimiter ;
2. represents a comma separated list of values, terminated by \n

The Janitor is just a repository that contains utilities functions, often in a temporary fashion.
