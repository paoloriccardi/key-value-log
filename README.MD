***
#General info about KeyValueLog
***

KeyValueLog is a Log based key:value datastore.
It was made for fun and educational purposes only.

***
#Key concepts
***

##Overall
New key:value tuples or new values for keys are just appended to a text file called Segment.
An in memory index of where is the last updated value for each key is kept and updated for faster reads. 
The index is a dictionary made of key:offset pairs, where the offset is the position where the last write of the key is located inside the Segment file.

###Basic assumptions and ideas: 
1. No key:value pair can be deleted once it has been inserted
2. Pairs are persisted appending them at the end of a textfile
3. To help reads an in-memory Index is mantained
4. We need to read just the most recent key:value pair

##File management
Despite the fact that a read operation returns just the last updated key:value pair, no key:value pair is e ever deleted from Segment files.
A Segment file can be compacted for practical reason when it reaches a certain size limit, the compaction generates a new Segment file which can be imported in a new Bucket generating a new in-memory index on the compacted Segment file. The older Segment files will be persisted and never touched or midified again.

##Classes entities and modules
A Bucket is the main component of the system. It has a Segment and keeps the in-memory Index. A Bucket provides an interface which allows the following:
1. write -> append the pair to the Segment file and updates the in-memory Index
2. read -> returns the pair key,element retrieved directly from the Segment file, using in-memory Index offset for key
3. delete -> writes key:tombstone, at the moment this operation is purely logical, no physical deletion intended

A Segment manages the Segment file and every operation related to it. A segment provides an interface which allows the following:
1. Append/retrieve of elements to/from the Segment file
2. Formatting functionalities which in some sense requires for each Element type to have its own specific Segment concrete implementations (which is not optimal)
3. Building or re-building the index dictionary from an existing Segment file, very useful for reinitiate a new instance, crash recovery or Segment file compactions (see Janitor)
4. Specific tombstone element for each Element type 
5. Facilities to close the current Segment file reference and very basic IO error handling 

An Element: key:value pair, where key is a string and value is a string that either:
1. represents a JSON object, encapsulated by {} and with element delimiter ;
2. represents a comma separated list of values, terminated by \n

A Janitor offers the compaction utilities to compact an existing Segment and generate a new Segment with a new Segment file. The older Segment is not modified and its preserved.
