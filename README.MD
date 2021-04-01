# General info about KeyValueLog Node

KeyValueLog is a Log based *key:value* datastore.

KVLNode is the Node component which hosts the Bucket, the Segment and provides a Rest API.

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


## Docker
The application is containerized. 
The *docker-compose.yml* file helps building a basic system with node and registry:
```
docker-compose up --build
```
