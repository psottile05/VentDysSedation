# from ipyparallel import Client
from pymongo import MongoClient, bulk
from sklearn.externals import joblib
import dask.dataframe as dd
import pandas as pd


__author__ = 'sottilep'

# ipclient = Client()
# print(ipclient.ids)
# ipview = ipclient.direct_view()

client = MongoClient()
db = client.VentDB
input_log = db.input_log
breath_col = db.breath_collection
bulk_ops = bulk.BulkOperationBuilder(breath_col, ordered = False)


def update_database(row):
    bulk_ops.find({'_id': row['_id']}).update_one({'$set': {'label.ds': row['label.ds']}})
    return 1

model = joblib.load('c:\Research_data\VD_models\ds\ds.pkl')

projection = {'breath_character.insp_time': 1,
              'breath_character.exp_time': 1,
              'breath_character.elapse_time': 1,
              'breath_character.min_exp_vol': 1,
              'breath_character.min_vol': 1,
              'next_breath_data.min_exp_vol': 1}

columns = ['_id', 'breath_character.insp_time', 'breath_character.exp_time',
           'breath_character.min_exp_vol', 'breath_character.min_vol',
           'next_breath_data.min_exp_vol']

breaths = breath_col.find({}, projection)

df = pd.io.json.json_normalize(list(breaths))
df.dropna(how = 'any', axis = 0, inplace = True)

print(df.columns)
data = list(df.columns)[1:]
labels = df['_id'].values
x = df[data].values

y = model.predict(x)
df['label.ds'] = y

df[['_id', 'label.ds']].apply(update_database, axis = 1)
bulk_ops.execute()
