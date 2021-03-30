## General info about KeyValueLog Gateway

KeyValueLog is a Log based *key:value* datastore.

KeyValueLog Gateway is the entry point for every request to a cluster of KVL Nodes.

## Registry
The Registry will have the task to keep a list of all the alive Nodes, to check their health and to allow them to register/deregister. The Registry will be the one to ask to locate the Node to query for a specific key.