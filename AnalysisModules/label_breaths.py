# from ipyparallel import Client
from pymongo import MongoClient
from sklearn.externals import joblib
from sklearn import metrics
from sklearn.naive_bayes import GaussianNB
import pandas as pd
import numpy as np

__author__ = 'sottilep'

# ipclient = Client()
# print(ipclient.ids)
# ipview = ipclient.direct_view()

client = MongoClient()
db = client.VentDB
input_log = db.input_log
breath_col = db.breath_collection

model = joblib.load('c:\Research_data\VD_models\ds\ds.pkl')

projection = {'breath_character.insp_time': 1,
              'breath_character.exp_time': 1,
              'breath_character.elapse_time': 1,
              'breath_character.min_exp_vol': 1,
              'breath_character.min_vol': 1,
              'next_breath_data.min_exp_vol': 1}

breaths = breath_col.find({}, projection).limit(10)

df = pd.io.json.json_normalize(list(breaths))
df.dropna(axis = 0, inplace = True, how = 'any')
data = list(df.columns)[1:]
labels = df['_id'].values
x = df[data].values

y = model.predict(x)
print(y)
