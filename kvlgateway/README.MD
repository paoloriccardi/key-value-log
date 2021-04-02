## General info about KeyValueLog Gateway
KeyValueLog is a Log based *key:value* datastore.

KeyValueLog Gateway is the entry point for every request to a cluster of KVL Nodes.

## Gateway responsibilities
The *Gateways* is an entry point for every client request in terms of read and write, it can be seen as a sort of API Gateway. It exposes a public Rest API and a private Rest API, the latter will need to be authenticated to allow connection only from internal nodes.

He is in charge of request routing, which are based on the requested key (in write or read mode). A read request will have only the reference to the *key* that a client wants to know the value of, the *Gateway* take care of sending the request to the correct *Bucket Node* and to route back the response to the client.

The *Gateway* receives an onboarding request from the Registry (on the private part of the API) whenever a new Node join the pool of available nodes, when the onboarding is complete, the Node will start to serve.

## The Onboarding process
1. A new *Bucket Node* B_n joins the pool and immediately after startup (to be precise right before bringing up its Flask Api) he contacts the Registry to register;
2. The *Registry* receives the request and after some internal check (like, do I already have registered a nod with the same Ip?) he sends an onboarding request to the *Gateway*
3. The *Gateway* receives the onboarding request and it does the following:
   - Calculate a hash for B_n
   - Insert B_n in his internal active node dictionary which has hash(B_n) as key
   - Insert hash(B_n) in an ordered linked list

# Serving requests
When a request for a *key* arrives to the *Gateway* the value of *hash(key)* is calculated. 

If *B[1..n]* is the list of the active buckets, then a *key* belongs to a Bucket *B[i]* if: *hash(key) < hash(B[i])* and there is no other *B[j]* such as *key < hash(B[j]) < hash(B[i])*

If there is no *B[i]* such as *hash(key)<B[i]* then *key* will be assigned to the bucket *B[k]* with the lower *hash(B[k])*. 