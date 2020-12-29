import json
from flask import Flask
from flask import Response
from flask import request
from flask_cors import CORS, cross_origin

import threading, time
from models import get_model,predict,data_conversion_for_predict

app = Flask(__name__)
app.config['SECRET_KEY'] = 'the quick brown fox jumps over the lazy dog'
app.config['CORS_HEADERS'] = 'Content-Type'

cors = CORS(app, resources={r"/forecast": {"origins": "http://localhost:5000"}})

@app.route('/forecast')
@cross_origin(origin='localhost',headers=['Content- Type','Authorization'])
def forecast():
    timestamp = request.args.get('timestamp')
    location_id = request.args.get('location_id')

    measurement = 0

    
    data = "{\"timestamp\": %s,\"location_id\": %s,\"measurement\": %f}" % (timestamp, location_id, measurement)
    print(predict(get_model(),data_conversion_for_predict(data)))
    print(data)
    return Response(
        response=data,
        status=200,
        mimetype='application/json'
    )

if __name__ == "__main__":
    app.run(host= '0.0.0.0')
