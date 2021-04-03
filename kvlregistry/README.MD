# General info about KeyValueLog Registry

At its core KeyValueLog is a Log based *key:value* datastore.

KeyValueLog Registry is the component against which the KVL Nodes registers for service.

## Registry
The *Registry* only responsibility is to be there when a Node spin up. Besides it will keep internally a list of the active Nodes, he could even check their health and allow them to register/deregister. 

So to be able to serve requests coming from the *Gateway*, every node will present itself to the *Registry* which will start the Onboarding process.

## The Onboarding process
1. A new *Bucket Node* *B[n]* joins the pool and immediately after startup (to be precise right before bringing up its Flask Api) he contacts the Registry to register;
2. The *Registry* receives the request and after some internal check (like, do I already have registered a nod with the same Ip?) he sends an onboarding request to the *Gateway*
3. The *Gateway* receives the onboarding request and it does the following:
   - Calculate a hash for *B[n]*
   - Insert *B[n]* in his internal active node dictionary which has *hash(B[n])* as key
   - Insert *hash(B[n])* in an ordered linked list