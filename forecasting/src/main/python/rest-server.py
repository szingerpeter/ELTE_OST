import json
from flask import Flask
from flask import Response
from flask import request
from flask import jsonify
from flask_cors import CORS, cross_origin

import threading, time
import os
from datetime import datetime

from cassandra.cluster import Cluster

from model import ForecastingModel

app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(12)
app.config['CORS_HEADERS'] = 'Content-Type'

cors = CORS(app, resources={r"/forecast": {"origins": "http://localhost:5000"}})

methods = ('GET', 'POST')

forecastingModel = ForecastingModel()

cassandra_session = None
while cassandra_session is None:
    try:
        # connect
        cassandra_session = Cluster(['cassandra'], port=9042).connect()
        time.sleep(5)
    except:
         pass

targets = list(range(0,1916))

@app.route('/forecast')
@cross_origin(origin='localhost',headers=['Content- Type','Authorization'])
def forecast():
    timestamp = request.args.get('timestamp')
    location_id = request.args.get('location_id')

    measurement = 0

    data = [float(timestamp),float(location_id)]
    data = "{\"timestamp\": %s,\"location_id\": %s,\"measurement\": %f}" % (timestamp, location_id, 
                                                                            forecastingModel.predict(data))
    return Response(
        response=data,
        status=200,
        mimetype='application/json'
    )

# GRAFANA SIMPLEJSON IMPLEMENTATION: https://grafana.com/grafana/plugins/grafana-simple-json-datasource

@app.route('/')
def index():
    print('index')
    return Response(response="Grafana JSON application", status=200)

@app.route('/search', methods=methods)
@cross_origin()
def find_metrics():
    print('search')
    return jsonify(targets)

@app.route('/annotations', methods=methods)
@cross_origin(max_age=600)
def query_annotations():
    print('annotations')
    
    return Response(response="Grafana JSON application", status=200)
    
@app.route('/query', methods=methods)
@cross_origin(max_age=600)
def query_metrics():
    req = request.get_json()

    from_str = req['range']['from']
    to_str = req['range']['to']

    from_date = datetime.strptime(from_str[:-5], '%Y-%m-%dT%H:%M:%S')
    to_date = datetime.strptime(to_str[:-5], '%Y-%m-%dT%H:%M:%S')

    from_unix = time.mktime(from_date.timetuple())
    to_unix = time.mktime(to_date.timetuple())

    timestamp = from_unix
    
    return_list = []

    interval = int((to_unix - from_unix) / 100)
    for target in req['targets']:
        t = target['target']

        target_dict = {}
        target_dict['target'] = t

        data = []

        rs = cassandra_session.execute('select timestamp, value from test.measurements where timestamp >= %d and timestamp <= %d and location_id = %d allow filtering;' % (from_unix, to_unix, int(t)))
        for row in rs:
            ts = row[0]
            value = row[1]
            data.append([value, int(ts*1000)])

        target_dict['datapoints'] = data

        pred_target_dict = {}
        pred_target_dict['target'] = t
        
        data = []

        while timestamp < to_unix:
            data.append([float(forecastingModel.predict([float(timestamp), float(t)])[0][0]), int(timestamp*1000)])
            timestamp += interval
        data.append([float(forecastingModel.predict([float(to_unix), float(t)])[0][0]), int(to_unix*1000)])
        
        pred_target_dict['datapoints'] = data

        return_list.append(target_dict)
        return_list.append(pred_target_dict)

    return jsonify(return_list)

if __name__ == "__main__":
    app.run(host= '0.0.0.0')