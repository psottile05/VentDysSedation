import pymongo
import pandas as pd
import numpy as np

client = pymongo.MongoClient()
db = client.VentDyssynchrony_db
ventSettings = db.VentSettings_collection
breathData = db.BreathData_collection


def custom_resampler(dict_like):
    return dict_like['analysis.ds'].sum(), dict_like['breath_number'].count(), dict_like['vent_settings.FiO2'].mean(), \
           dict_like['vent_settings.PEEP'].mean()


print(db.collection_names())
data = breathData.find({'patientID': 'P115'},
                       {'start_time': 1, 'breath_number': 1, 'vent_settings.PEEP': 1,
                        'vent_settings.FiO2': 1, 'analysis.ds': 1, '_id': 0})

df = pd.io.json.json_normalize(data)
df['start_time'] = pd.to_datetime(df['start_time'])
df.set_index('start_time', inplace = True)
print(df.columns)

resampled_df = df.resample('2H', how = custom_resampler)
test_df = resampled_df['analysis.ds'].shift(12)
resampled_df['ds_lag_24'] = test_df

print(df.describe())
print(resampled_df)
