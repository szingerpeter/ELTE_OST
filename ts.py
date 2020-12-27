import pandas as pd
import numpy as np

# ARIMA 
from statsmodels.tsa.arima.model import ARIMA

#TODO set correct name
NAME_FILE_CSV = "dataset.csv"
LOCATION_NAME_COLUMN = "location_id"
VALUE_NAME_COLUMN = "measurements"
TIME_NAME_COLUMN = "timestamp"

#TODO put correct request,until now only simulate it
location = 0 

#debug
DEBUG = 0

def get_prediction(location,steps):
    #read csv with pandas
    data = pd.read_csv(NAME_FILE_CSV,sep=",")

    #select rows by location
    data_by_location = data[data[LOCATION_NAME_COLUMN]==location]

    #remove location column
    data_by_location.drop(LOCATION_NAME_COLUMN,inplace=True, axis=1)

    #timestamp to datetime
    data_by_location[TIME_NAME_COLUMN] = pd.to_datetime(data_by_location[TIME_NAME_COLUMN], unit='s')


    #transform dataframe to timeseries
    series = pd.Series(np.array(data_by_location[VALUE_NAME_COLUMN]),index=data_by_location[TIME_NAME_COLUMN])
    series = series.asfreq(freq='5T')
    #plot and print values
    if(DEBUG==1):
        print(series)
        print("LEN: ", len(series))
        from matplotlib import pyplot
        series.plot()
        pyplot.show()

    
    # fit model
    model = ARIMA(series, order=(1, 1, 1))
    model_fit = model.fit()
    # make prediction
    yhat = model_fit.predict(len(series), len(series)+steps-1, typ='levels')
    return yhat


