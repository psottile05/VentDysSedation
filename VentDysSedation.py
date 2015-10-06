__author__ = 'sottilep'

from gevent import monkey
monkey.patch_all()

from pymongo import MongoClient, errors
from CreationModules import FileSearch as FS
from CreationModules import DatabaseCreation as DBCreate

import json

client = MongoClient()
db = client.VentDB

input_log = db.input_log
breath_col = db.breath_collection
breath_col.drop()

# Update List of RawDataFiles
FS.file_search()

# Query DB for list of files not yet added
files = list(input_log.find({'type': 'waveform', 'loaded': 0}).limit(1))

for file in files:
    df = DBCreate.get_waveform_data(file)
    breath_col.insert_many(json.loads(df.groupby('breath').apply(DBCreate.breath_data_entry).to_json(orient='records')))

    df = DBCreate.get_breath_data(file)
    print(file)

print(breath_col.find().count())