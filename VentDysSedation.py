__author__ = 'sottilep'

from pymongo import MongoClient, errors
from CreationModules import FileSearch as FS
import pandas as pd
import numpy as np

client = MongoClient()
db = client.VentDB
input_log = db.input_log

#Update List of RawDataFiles
FS.file_search()

#Query DB for list of files not yet loaded
files = list(input_log.find({'type':'waveform', 'loaded':0}).limit(2))

for file in files:
    df = pd.read_csv(file['_id'], sep = '\t', header=1, na_values='--',
                     usecols=['Date', 'HH:MM:SS', 'Time(ms)', 'Breath', 'Status', 'Paw (cmH2O)', 'Flow (l/min)', 'Volume (ml)'])
    df['date_time'] = pd.to_datetime(df['Date'] +' '+ df['HH:MM:SS'], coerce=True, dayfirst=True, infer_datetime_format=True)
    df.drop(['Date', 'HH:MM:SS'], axis=1, inplace=True)
    df.rename(columns={'Time(ms)': 'time', 'Breath': 'breath', 'Status': 'status', 'Paw (cmH2O)': 'paw',
                       'Flow (l/min)': 'flow', 'Volume (ml)': 'vol'}, inplace=True)

    breath_df = df.groupby('breath')
    print(len(breath_df.groups))
    agg_df = breath_df.agg({'date_time': [np.min],
                            'time':[np.min, np.max, lambda x: np.max(x)-np.min(x)],
                            'paw':[np.min, np.max],
                            'flow': [np.min, np.max],
                            'vol':[np.min, np.max]})
    print(agg_df.columns)
    print(agg_df.head())
    #print(breath_df.head())