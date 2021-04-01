from flask import request, jsonify, abort, Flask
import json
import requests

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify("KVL Gateway!")

@app.route('/api/v1/elements/', methods=['GET'])
def read():
    if 'key' not in request.args:
        abort(501)

    key = str(request.args['key'])
    
    nodeIp = "node1"
    nodePort = "5001"
    nodeEndpoint = "http://" + nodeIp + ":" + nodePort + "/api/v1/elements/?key="+key
    try:
        nodeResponse = requests.get(nodeEndpoint)
    except Exception as err:
        print("An error occurred connecting to Registry" + " > " + str(err))

    return jsonify(nodeResponse.json())
    
@app.route('/api/v1/elements/', methods=['POST'])
def write():
    if not request.json or not request.json['key'] or not request.json['value']:
        abort(501)
    key = request.json['key']
    value = request.json['value']
    
    return jsonify(key+value)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=7001)