from flask import request, jsonify, abort, Flask
import json

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify("Hallo world!")


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5001)