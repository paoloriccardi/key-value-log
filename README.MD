## General info about KeyValueLog Registry

KeyValueLog is a Log based *key:value* datastore.

KeyValueLog Registry is the component against which the KVL Nodes registers for service.

## Registry
The Registry will have the task to keep a list of all the alive Nodes, to check their health and to allow them to register/deregister. The Registry will be the one to ask to locate the Node to query for a specific key.