__author__ = 'sottilep'

from pymongo import MongoClient
import pandas as pd

client = MongoClient()
print(client.database_names())

db = client.VentDyssynchrony_db
print(db.collection_names())

breath_data = db.BreathData_collection
rn_data = db.RNData_collection

patient = 'P104'
results = breath_data.find({'patientID': patient},
                           {'_id': 0, 'patientID': 1, 'start_time': 1, 'analysis': 1}).limit(1)
breath_df = pd.io.json.json_normalize(results)
breath_df.set_index(['start_time'], inplace = True)
breath_df['patientID'] = breath_df['patientID'].str.lstrip('P').astype(int)
breath_df.resample('1s', limit = 120)

results = rn_data.find({'patientID': patient, 'RN_entry.RASS': {'$exists': 1}},
                       {'_id': 0, 'patientID': 1, 'entry_time': 1, 'RN_entry': 1}).limit(1)
[print(x) for x in results]
