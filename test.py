from gevent import monkey

monkey.patch_all()

import pandas as pd
import dask.dataframe as dd
from pymongo import MongoClient

client = MongoClient()
db = client.VentDB
input_log = db.input_log
breath_col = db.breath_collection

results = breath_col.find({}, {'breath_raw': 0, 'max_min_raw': 0, 'breath_derivative': 0, 'loc': 0}).limit(100000)
df = pd.io.json.json_normalize(list(results))
df.to_hdf('c:\\Research_data\\Analysis\\breath_dump_4_26_16.h5', key = 'breath_collection', format = 'table',
          append = False)

ddf = dd.from_pandas(df, npartitions = 1000)
ddf.to_hdf('c:\\Research_data\\Analysis\\dask_breath_dump_4_26_16.h5', key = 'breath_collection')
