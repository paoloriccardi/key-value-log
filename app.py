from flask import request, jsonify, abort, Flask
import json

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify("Hallo world!")

@app.route('/api/v1/elements/', methods=['GET'])
def read():
    return jsonify("Ok")


@app.route('/api/v1/elements/', methods=['POST'])
def write():
    if not request.json or not request.json['key'] or not request.json['value']:
        abort(501)
    key = request.json['key']
    value = request.json['value']
    
    return jsonify(key+value)



if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5001)