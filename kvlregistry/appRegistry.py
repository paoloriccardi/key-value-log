from flask import request, jsonify, abort, Flask
import json

from KVLRegistry import KVLRegistry, KVLRegistryEntry
serviceR = KVLRegistry()

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify("KVL Registry")

@app.route('/api/v1/registry/', methods=['DELETE'])
def unregister():
    if not request.json or not request.json['ip'] or not request.json['port']:
        abort(501)
    ip = request.json['ip']
    port = request.json['port']
    entry = KVLRegistryEntry(ip,port)
    if serviceR.unregister(entry):
        return jsonify("Node Unregistered")
    else:
        abort(501)
    
@app.route('/api/v1/registry/', methods=['POST'])
def register(): 
    if not request.json or not request.json['ip'] or not request.json['port']:
        abort(400)
    ip = request.json['ip']
    port = request.json['port']
    entry = KVLRegistryEntry(ip,port)
    success = serviceR.register(entry) # register tries to connect to Gateway to complete the onboarding of the node
    if success:
        return jsonify("Registered")
    else:
        abort(501)

@app.route('/api/v1/registry/nodes/all/', methods=['GET'])
def getAll():
    entryList = []
    #should check and return just the active nodes
    for element in serviceR.registry:
        entryList.append(element.toJSON())
    return jsonify(entryList)

@app.route('/api/v1/registry/nodes/', methods=['GET'])
def getNodeDetails():
    if 'ip' in request.args and 'port' in request.args:
        ip = str(request.args['ip'])
        port = str(request.args['port'])
        entry = KVLRegistryEntry(ip,port)
        details = serviceR.getRegistered(entry)
        return jsonify(details)
    abort(501)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=6001)