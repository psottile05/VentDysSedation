__author__ = 'sottilep'

import datetime
import re
from ipyparallel import Client
import pymongo
from pymongo import MongoClient
from CreationModules import FileSearch as FS

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


@ipview.parallel()
def make_waveform_and_breath(files):
    from CreationModules import DatabaseCreation as DBCreate
    for file in files:
        DBCreate.get_waveform_and_breath(file)


# @ipview.parallel()
def make_EHR_data(files):
    from CreationModules import EHR_decoder as EHR
    for file in files:
        EHR.load_EHR_data(file['_id'], file['patient_id'])

# Query DB for list of Waveform/breath files not yet added
files = list(input_log.find({'type': 'waveform', 'loaded': 0}).limit(3))
make_waveform_and_breath(files)

# Query DB for list of EHR files not yet added
files = list(input_log.find({'$and': [{'type': {'$not': re.compile(r'waveform')}},
                                      {'type': {'$not': re.compile(r'breath')}},
                                      {'type': {'$not': re.compile(r'other')}},
                                      {'loaded': 0}]}, {'_id': 1, 'patient_id': 1}).limit(3))
make_EHR_data(files)


for items in input_log.find({'loaded': 1}):
    print(items)
