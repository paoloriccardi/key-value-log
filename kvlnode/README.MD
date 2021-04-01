# General info about KeyValueLog Node

KeyValueLog is a Log based *key:value* datastore.

KeyValueLog Node is the Node component which hosts the Bucket, the Segment and provides a Rest API.

The core components, Bucket and Segment, are in a dedicated directory together with a files directory, where the segment files will be saved.
The API relies on Flask and the Node is dockerized for easy of use.

*Everything here was made for fun and educational purposes only.*

## Overall
New *key:value* tuples are appended to a text file called Segment File.
An in memory index of where the last updated value is located inside the file for each key is kept and updated for faster reads. 
The index is a dictionary made of key:offset pairs.

### Basic assumptions and ideas 
1. No key:value pair is ever deleted from the segment files, once it has been inserted
2. Pairs are persisted appending them at the end of a plain textfile 
3. To improve reads an in-memory Index is mantained
4. We assume that we need to read just the most recent key:value pair

## Classes entities and modules
A Bucket is the main component of the system. It has a Segment, which manage the segment file, and keeps the in-memory Index for the segment contents. 
The Bucket provides the following interface:
1. write a new *key:value* pair
2. read the most recent *value* for a given *key*
3. delete (logical delete, a *tombstone* is written in the value for the key)
4. compact (see file management below)
5. reload a bew Segment discarding the old one
6. inizialize a new bucket given a dictinoary of *key:value* pairs

A segment take care of all the file operations underlying the bucket. A segment provides the following interface:
1. Append/retrieve of elements to/from the Segment file
2. Formatting functionalities according to the kind of *value* we want to persist
3. Building or re-building the index dictionary from an existing Segment file
4. Providing specific tombstone element for the kind of *value* managed 
5. Segment files scanning utilities 
6. Compaction utilities
7. Initialization utilities
8. File Shrinking utilities

There are two concrete implementations of Segment:
1. JSON
2. Simple Value

## File management
The active Segment file can be compacted for practical reason when it reaches a certain size limit. The compaction of a Segment involves:
1. The bucket asks to his segment to compact itself
2. The segment detach the old file and create a new one, where he will happen only the most recent version of overy *key:value*
3. The segment will return to his bucket a newly generated Index to be stored in memory for future queries
4. The older files are never deleted in the process, after compaction a bucket will use and modify a new file

## Docker
The application is containerized. 
To build the docker image from the Dockerfile you can just type in the main project directory:
```
docker build --tag kvl-docker .
```
In order to run the container, from the same directory you can type:
```
docker run --name kvl-docker -p 5001:5001 kvl-docker
```
Please refer to Flask and Docker documentation for any details.