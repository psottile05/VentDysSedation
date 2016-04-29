from pymongo import MongoClient
import pandas as pd
import numpy as np

client = MongoClient()
db = client.VentDB
input_log = db.input_log

results = input_log.find({}, {'patient_id': 1, 'type': 1, 'elapse_time_h': 1})

df = pd.io.json.json_normalize(list(results))
print(df.groupby(['patient_id', 'type']).describe())
print(df[df.type == 'waveform'].groupby(['patient_id']).sum())

print(df)
