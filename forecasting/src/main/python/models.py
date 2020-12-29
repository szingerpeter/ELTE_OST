from numpy import array
from keras.models import Sequential
from keras.layers import LSTM
from keras.layers import Dense
import json
import numpy as np

#training part
#https://colab.research.google.com/drive/1MQP2LBgiPOlOa4V1HQcAfSjH1XcAsjRx?usp=sharing

def get_model():
    model = Sequential()
    model.add(LSTM(50, return_sequences=False,activation='relu', input_shape=(2,1)))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')
    model.load_weights("weight.h5")
    return model

def retrain_model(model,new_data):
    print(type(new_data))
    new_entry_X = np.expand_dims(new_data[0:2], axis=-1)
    new_entry_X = np.expand_dims(new_entry_X, axis=0)

    new_entry_y = np.array(new_data[2])
    model.fit(np.array(new_entry_X),np.array(np.expand_dims(new_entry_y, axis=0)),epochs=1,batch_size=1,verbose=1)
    model.save_weights("weight.h5")

def data_conversion_for_train(data):
  data = json.loads(data)
  return [float(data["timestamp"]),float(data["location_id"]),float(data["measurement"])]

def data_conversion_for_predict(data):
  data = json.loads(data)
  return [float(data["timestamp"]),float(data["location_id"])]

def predict(model,data):
  t = np.expand_dims(data, axis=-1)
  t = np.expand_dims(t, axis=0)
  return model.predict(t)

#example
#print(retrain_model(get_model(),data_conversion_for_train("{\"timestamp\":1.2807108E9,\"location_id\":0,\"measurement\":332.8999938964844}")))
#print(predict(get_model(),data_conversion_for_predict("{\"timestamp\":1.2807108E9,\"location_id\":0}")))