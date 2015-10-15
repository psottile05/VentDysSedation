__author__ = 'sottilep'

from gevent import monkey

# monkey.patch_all()

from pymongo import MongoClient
from CreationModules import FileSearch as FS
from CreationModules import DatabaseCreation as DBCreate

import json
import pandas as pd
import pymongo
import datetime
import re
# import gevent
# from gevent.lock import Semaphore

pd.set_option('max_columns', 40)
client = MongoClient()
db = client.VentDB

input_log = db.input_log
breath_col = db.breath_collection

input_log.drop()
breath_col.drop()

input_log.create_index([('type', pymongo.TEXT)])
input_log.create_index([('loaded', pymongo.ASCENDING)])
input_log.create_index([('analyzed', pymongo.ASCENDING)])
input_log.create_index([('loc', pymongo.GEO2D)], min = -1,
                       max = (datetime.datetime.now() + datetime.timedelta(days = 1440)).timestamp())

breath_col.create_index([('patient_ID', pymongo.ASCENDING)])
breath_col.create_index([('file', pymongo.TEXT)])
breath_col.create_index([('breath_num', pymongo.ASCENDING)])
breath_col.create_index([('date_time', pymongo.ASCENDING)])
breath_col.create_index([('loc', pymongo.GEO2D)], min = -1,
                        max = (datetime.datetime.now() + datetime.timedelta(days = 1440)).timestamp())


# Update List of RawDataFiles and Match Breath/Waveform Files
FS.file_search()
FS.file_match()

# Query DB for list of files not yet added
files = list(input_log.find({'type': 'waveform', 'loaded': 0}).limit(1))

for file in files:
    print(file['_id'])
    wave_df = DBCreate.get_waveform_data(file)
    breath_col.insert_many(
        json.loads(wave_df.groupby('breath').apply(DBCreate.waveform_data_entry).to_json(orient = 'records')))
    input_log.update_one({'_id': file['_id']}, {'$set': {'loaded': 1}})

    bulk_breath = breath_col.initialize_unordered_bulk_op()
    breath_df = DBCreate.get_breath_data(file)
    breath_df.apply(DBCreate.breath_data_entry, axis = 1, match_file = re.search(r'(?<=\d/).*', file['_id']).group(),
                    bulk_breath = bulk_breath)
    input_log.update_one({'_id': file['match_file']}, {'$set': {'loaded': 1, 'crossed': 1}})

bulk_breath.execute()
