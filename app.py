from flask import request, jsonify, abort, Flask
import json

from KVLRegistry import KVLRegistry, KVLRegistryEntry

app = Flask(__name__)

registry = KVLRegistry()

@app.route('/')
def home():
    return jsonify("Hallo world!")

@app.route('/api/v1/nodes/', methods=['GET'])
def read():
    return jsonify("Ok")


@app.route('/api/v1/registry/', methods=['DELETE'])
def unregister():
    if not request.json or not request.json['ip'] or not request.json['port']:
        abort(501)
    ip = request.json['ip']
    port = request.json['port']
    entry = KVLRegistryEntry(ip,port)
    if registry.unregister(entry):
        return jsonify("Node Unregistered")
    else:
        abort(501)
    
@app.route('/api/v1/registry/', methods=['POST'])
def register():
    if not request.json or not request.json['ip'] or not request.json['port']:
        abort(501)
    if request.remote_addr != request.json['ip']:
        abort(501)
    
    ip = request.json['ip']
    port = request.json['port']

    entry = KVLRegistryEntry(ip,port)
    registry.register(entry)
    
    return jsonify("Registered")

@app.route('/api/v1/registry/nodes/all/', methods=['GET'])
def getAll():
    return jsonify("Ok")

@app.route('/api/v1/registry/nodes/', methods=['GET'])
def getNodeDetails():
    return jsonify("Ok")


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=6001)