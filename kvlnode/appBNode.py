from flask import request, jsonify, abort, Flask
import json
import requests
import socket

from threading import Timer


from core.KVLBucket import KVLBucket
from core.KVLSegment import KVLSegmentSimpleValue
from core.KVLSegment import KVLSegmentJSON

filename = "segmentfile.txt"
segment = KVLSegmentSimpleValue(filename)
bucket = KVLBucket(segment)

app = Flask(__name__)
#app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    return "KVL Bucket"

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
        abort(400)
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
        abort(501)

@app.route('/api/v1/elements/all/', methods=['GET'])
def getAllElements():
    elements = bucket.index
    keylist = []
    for key in elements.keys():
        keylist.append(key)
    return jsonify(keylist)

    

@app.route('/api/v1/elements/', methods=['POST'])
def appendElement():
    if not request.json or not request.json['key'] or not request.json['value']:
        abort(400)
    key = request.json['key']
    value = request.json['value']
    bucket.write(key,value)
    return jsonify("Ok")




nodeIp = str(socket.gethostbyname(socket.gethostname()))
nodePort = "5001"

#Callback for the registration to the Registry
def sendRegistration(ip,port):
    serviceRegistryIp = "registry"
    serviceRegistryPort = "6001"

    nodeEndpoint = "http://" + serviceRegistryIp + ":" + serviceRegistryPort + "/api/v1/registry/"
    head = {'Accept' : 'application/json', 'Content-Type' : 'application/json'}
    jsondata = json.dumps({'ip':ip,'port':port}) 

    try:
        requests.post(nodeEndpoint,data=jsondata,headers=head)
    except Exception as err:
        print("An error occurred connecting to Registry" + " > " + str(err))

#send registration (hopefully) after node's API has started
networkInstance = True
if networkInstance:
    t = Timer(60.0,sendRegistration,[nodeIp,nodePort])
    t.start()


if __name__ == "__main__":
    #app.run(host="0.0.0.0", port=nodePort)
    app.run(host=nodeIp, port=nodePort)
