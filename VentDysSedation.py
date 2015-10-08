__author__ = 'sottilep'

from gevent import monkey
monkey.patch_all()

from pymongo import MongoClient
from CreationModules import FileSearch as FS
from CreationModules import DatabaseCreation as DBCreate

import json
import pandas as pd
import pymongo
import datetime
# import gevent
# from gevent.lock import Semaphore

pd.set_option('max_columns', 40)
client = MongoClient()
db = client.VentDB

input_log = db.input_log
breath_col = db.breath_collection
input_log.drop()
breath_col.drop()

breath_col.create_index([('patient_ID', pymongo.ASCENDING),
                         ('file', pymongo.TEXT),
                         ('breath_num', pymongo.ASCENDING),
                         ('date_time', pymongo.ASCENDING)])
breath_col.create_index([('location', pymongo.GEO2D)], min = -1,
                        max = (datetime.datetime.now() + datetime.timedelta(days = 1440)).timestamp())

# Update List of RawDataFiles and Match Breath/Waveform Files
FS.file_search()
FS.file_match()

# Query DB for list of files not yet added
files = list(input_log.find({'type': 'waveform', 'loaded': 0}).limit(1))

for file in files:
    wave_df = DBCreate.get_waveform_data(file)
    breath_col.insert_many(
        json.loads(wave_df.groupby('breath').apply(DBCreate.waveform_data_entry).to_json(orient = 'records')))

    breath_df = DBCreate.get_breath_data(file)

    # print(file)
