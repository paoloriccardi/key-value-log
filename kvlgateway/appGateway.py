from flask import request, jsonify, abort, Flask

from KVLGateway import KVLGateway
import socket

gw = KVLGateway('registry','6001')

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify("KVL Gateway!")

@app.route('/api/v1/elements/', methods=['GET'])
def read():
    if 'key' not in request.args:
        abort(400)
    key = str(request.args['key'])

    #node routing
    nodeHash = gw.resolveKeyToNode(key)
    node = gw.nodes[nodeHash]

    jsonResponse = gw.routeReadToNode(node['ip'],node['port'],key)    
    return jsonify(jsonResponse)
    
@app.route('/api/v1/elements/', methods=['POST'])
def write():
    if not request.json or not request.json['key'] or not request.json['value']:
        abort(400)
    kvdict = {'key':request.json['key'],'value':request.json['value']}

    #node routing
    nodeHash = gw.resolveKeyToNode(kvdict['key'])
    node = gw.nodes[nodeHash]

    jsonResponse = gw.routeWriteToNode(node['ip'],node['port'],kvdict)
    if jsonResponse != {} :
        return jsonify(jsonResponse)
    else:
        abort(501)

@app.route('/api/v1/elements/all/', methods=['GET'])
def getAllElements():
    keys = gw.gatherAllKeys()
    return jsonify(keys)


@app.route('/api/v1/internals/nodes/', methods=['GET'])
def internalNodes():
    return jsonify(gw.nodes)

@app.route('/api/v1/internals/nodes/keydistribution/', methods=['GET'])
def internalKeyDistribution():
    response = {}
    for key,node in gw.nodes.items():
        response[key] = len(gw.getKeysFromNode(node['ip'],node['port']))
    return jsonify(response)


#Private API section
@app.route('/api/v1/conf/onboard/', methods=['POST'])
def onboarding():
    if not request.json or not request.json['ip'] or not request.json['port'] :
        abort(400)
    success = gw.onBoardingNode(request.json['ip'], request.json['port'])
    if success:
        return jsonify("Node Added")
    else:
        abort(501)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=7001)