# General info about KeyValueLog 

At its core KeyValueLog is a Log based *key:value* datastore.

But before we start... 

## A word of caution
The only rule I applied throughout this projet is to deliberately reinvent the wheel whenever possible.

To quote (more or less faithfully) a sentence I heard from the wise guy that started Redis: 
>If everybody had stuck to the rule "never reinvent the wheel" F1 cars today would still use wooden wheels like those chariots in ancient Rome 

My idea is that reinventing the wheel can help to improve the state of the art, which is definitely **not** the purpose of this project, but it can also improve our awareness on how wheels can be made, on how they should work and why. 

*So please note: Everything here was made for fun and educational purposes only.*

## Overall
The core application is pretty simple, it's made of few python classes and can be used as a standalone module, but the whole eco-system built around it provides other functionalities to use it in a Service or as a Service.

The main components are:
- KVLNode is the single Node component, it can be accessed via a Rest API which provides the basic operations on the local datastore
- KVLRegistry is the Service Registry component, it's where available nodes register themselves
- KVLGateway is the API Gateway for external clients

All the APIs at the moment relies purely on Flask.

Every node, or bucket node, will keep a portion of the "space of the keys" (the possible values for the keys) and will persist the evolution of the *key:value* pair with an append only logic. A Node is made of a Bucket class accessed via a Rest API. The Bucket *has a* Segment, which will hold the reference to an actual file on disk where the *key:value* pairs will be stored, and an in memory Index, used to quickly locate the *key:value* in the file for every *key*.

The bucket nodes will not be queried directly from the clients but they will be intermediated by an Api Gateway, who will provide a sort of Facade for them.

The bucket nodes register themselves on startup against a Service Registry.

### Basic assumptions about the storage
1. No key:value pair is ever deleted from the segment files, once it has been inserted
2. Pairs are persisted appending them at the end of a plain textfile 
3. The in-memory Index is used to speed up access to *key:value* pairs
4. Only the most recent *key:value* pair are relevant, the older versions are kept but never accessed or modified

## Docker
The application is containerized. 
The *docker-compose.yml* file helps building a basic system with node and registry:
```
docker-compose up --build
```
