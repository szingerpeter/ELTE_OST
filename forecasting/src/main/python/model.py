from numpy import array
from keras.models import Sequential
from keras.layers import LSTM
from keras.layers import Dense,Dropout
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
    self.input_lenght = 1917 # cause by one-hot-encoding
    
    self.model = Sequential()
    self.model.add(Dense(self.input_lenght, input_dim=self.input_lenght, activation='relu'))
    self.model.add(Dense(1024, activation='relu'))
    self.model.add(Dropout(0.3))
    self.model.add(Dense(1024, activation='relu'))
    self.model.add(Dense(1))
    self.model.compile(optimizer='adam', loss='mse')
    with self.graph.as_default():
      with self.session.as_default():
        self.model.load_weights(self.weights_filename)

  def get_model(self):
      return self.model

  
  def one_hotencode(self,number):
      array = np.zeros((self.input_lenght-1), dtype=int)
      array[number]=1
      return array

  def convert_second_of_the_day(self,timestamp):
      return timestamp % 86400

  def retrain(self,data):
    with self.graph.as_default():
      with self.session.as_default():
        data = json.loads(data)
        data = [float(data["timestamp"]),float(data["location_id"]),float(data["measurement"])]
          
        new_entry_X = np.expand_dims(np.concatenate((np.array(self.convert_second_of_the_day(data[0])), self.one_hotencode(int(data[1]))), axis=None) ,axis=0)
        new_entry_y = np.expand_dims(data[2], axis=0)

        self.model.fit(new_entry_X,new_entry_y,epochs=1,verbose=1)
        self.model.save_weights(self.weights_filename)

    return self.model

  def predict(self,data):
    with self.graph.as_default():
      with self.session.as_default():
        self.model.load_weights(self.weights_filename)
        to_predict = np.expand_dims(np.concatenate((np.array(self.convert_second_of_the_day(data[0])), self.one_hotencode(int(data[1]))), axis=None) ,axis=0)
        pred = self.model.predict(to_predict.reshape(1,1917))
    return pred

