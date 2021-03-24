import flask
from flask import request, jsonify, abort

from KVLJanitor import KVLJanitor

filename = "example.txt"
janitor = KVLJanitor()
bucket = janitor.createBucket(filename)

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    return "<h1>Key Value Log - Bucket</h1>"

@app.route('/api/v1/heartbeat/', methods=['GET'])
def heartbeat():
    return jsonify("Alive")

@app.route('/api/v1/internals/index/', methods=['GET'])
def index():
    return jsonify(bucket.index)

@app.route('/api/v1/elements/', methods=['GET'])
def elementbykey():
    if 'key' in request.args:
        key = str(request.args['key'])
    else:
        abort(501)
    element = bucket.read(key)
    return jsonify(element)

@app.route('/api/v1/elements/', methods=['POST'])
def append():
    if not request.json or not request.json['key'] or not request.json['value']:
        abort(501)
    key = request.json['key']
    value = request.json['value']
    bucket.write(key,value)
    return jsonify("Ok")

app.run()