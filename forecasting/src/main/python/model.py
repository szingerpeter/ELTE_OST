from numpy import array
from keras.models import Sequential
from keras.layers import LSTM
from keras.layers import Dense
import keras
import tensorflow as tf
import json
import numpy as np

#training
#https://colab.research.google.com/drive/1MQP2LBgiPOlOa4V1HQcAfSjH1XcAsjRx?usp=sharing

class ForecastingModel():

  def __init__(self):
    self.session = tf.Session()
    self.graph = tf.get_default_graph()
    self.weights_filename = "/opt/app/src/main/python/resources/weights.h5"
    self.model = Sequential()
    self.model.add(LSTM(50, return_sequences=False,activation='relu', input_shape=(2,1)))
    self.model.add(Dense(1))
    self.model.compile(optimizer='adam', loss='mse')
    with self.graph.as_default():
      with self.session.as_default():
        self.model.load_weights(self.weights_filename)

  def get_model(self):
      return self.model

  def retrain(self, data):
      data = json.loads(data)
      data = [float(data["timestamp"]),float(data["location_id"]),float(data["measurement"])]

      new_entry_X = np.expand_dims(data[0:2], axis=-1)
      new_entry_X = np.expand_dims(new_entry_X, axis=0)

      new_entry_y = np.array(data[2])
      self.model.fit(np.array(new_entry_X),np.array(np.expand_dims(new_entry_y, axis=0)),epochs=1,batch_size=1,verbose=1)
      self.model.save_weights(self.weights_filename)

      return self.model

  def predict(self,data):
    with self.graph.as_default():
      with self.session.as_default():
        self.model.load_weights(self.weights_filename)
        t = np.expand_dims(data, axis=-1)
        t = np.expand_dims(t, axis=0)
        pred = self.model.predict(t)
    return pred

#example
#print(retrain(get_model(),data_conversion_for_train("{\"timestamp\":1.2807108E9,\"location_id\":0,\"measurement\":332.8999938964844}")))
#print(predict(get_model(),data_conversion_for_predict("{\"timestamp\":1.2807108E9,\"location_id\":0}")))