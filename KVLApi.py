from flask import request, jsonify, abort, Flask
import json
import requests
import time


from core.KVLBucket import KVLBucket
from core.KVLSegment import KVLSegmentSimpleValue
from core.KVLSegment import KVLSegmentJSON

filename = "logfile.txt"
segment = KVLSegmentSimpleValue(filename)
bucket = KVLBucket(segment)

app = Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    return "<h1>Key Value Log - Bucket</h1>"

@app.route('/api/v1/internals/heartbeat/', methods=['GET'])
def heartbeat():
    return jsonify("Alive")

@app.route('/api/v1/internals/index/', methods=['GET'])
def index():
    return jsonify(bucket.index)

@app.route('/api/v1/internals/compact/', methods=['GET'])
def compactSegment():
    bucket.compact()
    return jsonify(bucket.index)

@app.route('/api/v1/internals/initialize/', methods=['POST'])
def initializeBucket():
    if not request.json:
        abort(501)
    kvdict = request.json
    bucket.initializeBucket(kvdict)    
    return jsonify("Ok")

@app.route('/api/v1/elements/', methods=['GET'])
def getElementbykey():
    if 'key' in request.args:
        key = str(request.args['key'])
        element = bucket.read(key)
        return jsonify(element)
    else:
        elements = bucket.index
        keylist = []
        for key in elements.keys():
            keylist.append(key)
        return jsonify(keylist)
    

@app.route('/api/v1/elements/', methods=['POST'])
def appendElement():
    if not request.json or not request.json['key'] or not request.json['value']:
        abort(501)
    key = request.json['key']
    value = request.json['value']
    bucket.write(key,value)
    return jsonify("Ok")



serviceRegistryIp = "192.168.112.1"
serviceRegistryPort = "6001"
nodeIp = "192.168.112.1"
nodePort = "5001"
nodeEndpoint = "http://" + serviceRegistryIp + ":" + serviceRegistryPort + "/api/v1/registry/"
try:
    nodeResponse = requests.post(nodeEndpoint,json={"ip":nodeIp,"port":nodePort})
except Exception as err:
    print("An error occurred connecting to Registry" + " > " + str(err))   

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5001)