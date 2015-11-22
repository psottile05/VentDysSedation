__author__ = 'sottilep'

# from gevent import monkey
# monkey.patch_all()

import datetime
import json
import re
from ipyparallel import Client

import pandas as pd
import pymongo
from pymongo import MongoClient

from CreationModules import DatabaseCreation as DBCreate
from CreationModules import FileSearch as FS

pd.set_option('max_columns', 40)

ipclient = Client()
print(ipclient.ids)
ipview = ipclient.load_balanced_view()

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


@ipview.parallel(block = True)
def get_waveform_and_breath(file):
    breath_df = DBCreate.get_breath_data(file)
    wave_df = DBCreate.get_waveform_data(file)

    breath_col.insert_many(
        json.loads(
            wave_df.groupby('breath', sort = False).apply(DBCreate.waveform_data_entry, breath_df = breath_df).to_json(
                orient = 'records')), ordered = False)
    input_log.update_one({'_id': file['_id']}, {'$set': {'loaded': 1}})
    input_log.update_one({'_id': file['match_file']}, {'$set': {'loaded': 1, 'crossed': 1}})


# Query DB for list of Waveform/breath files not yet added
files = list(input_log.find({'type': 'waveform', 'loaded': 0}).limit(8))

for file in files:
    # print(file['_id'])
    get_waveform_and_breath(file)

# Query DB for list of EHR files not yet added
files = list(input_log.find({'$and': [{'type': {'$not': re.compile(r'waveform')}},
                                      {'type': {'$not': re.compile(r'breath')}},
                                      {'type': {'$not': re.compile(r'other')}},
                                      {'loaded': 0}]}, {'_id': 1, 'patient_id': 1}))

for file in files:
    pass
    #print(file)

# wave_and_breath_greenlets = [gevent.spawn(get_waveform_and_breath, file, Semaphore(100)) for file in files]
# gevent.joinall(wave_and_breath_greenlets)

print(breath_col.find_one())
