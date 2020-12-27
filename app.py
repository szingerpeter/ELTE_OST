from ts import get_prediction
from ts import TIME_NAME_COLUMN,VALUE_NAME_COLUMN
from flask import Flask
from flask import request
from flask import jsonify
import numpy as np

app = Flask(__name__)

'''
@app.route('/')
def hello():
    return get_prediction(0)
'''

@app.route('/prediction')
def prediction():
    #request
    location = request.args.get('location')
    steps = request.args.get('steps')

    #transform to json
    p = get_prediction(int(location),int(steps))
    data = p.reset_index(level=0)
    data.rename(columns = {'index':TIME_NAME_COLUMN}, inplace = True) 
    data.rename(columns = {'predicted_mean':VALUE_NAME_COLUMN}, inplace = True) 
    data[TIME_NAME_COLUMN] = data[TIME_NAME_COLUMN].values.astype(np.int64) // 10 ** 9
    
    return jsonify(data.to_json(orient="records"))

if __name__ == '__main__':
    app.run()
