from KVLRegistry import KVLRegistryEntry

testCheckNodeAlive = True
if testCheckNodeAlive:

    #wrong port provided
    entry = KVLRegistryEntry('localhost','5002')
    print(entry.toJSON())
    if entry.check():
        print ("Node " + entry.ip + ":"+ entry.port + " is Alive")
    else:
        print ("Node " + entry.ip + ":"+ entry.port + " is Dead")

    #right port:ip provided
    entry = KVLRegistryEntry('localhost','5001')
    print(entry.toJSON())
    if entry.check():
        print ("Node " + entry.ip + ":"+ entry.port + " is Alive")
    else:
        print ("Node " + entry.ip + ":"+ entry.port + " is Dead")


    