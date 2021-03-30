class KVLRegistry:
    def __init__(self):
        self.registry = {}

    def register(self):
        #Nodes can register to the registry (they will try to register at startup)
        return

    def unregister(self):
        #Nodes can unregister from the registry
        return

    def healthcheck(self):
        #Nodes notify the Registry that they are alive
        return

    def resolve(self):
        #resolve the association between key and the node currently hosting that key
        return
