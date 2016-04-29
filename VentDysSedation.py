import datetime
import re
import pymongo
from pymongo import MongoClient
# from ipyparallel import Client

from CreationModules import FileSearch as FS
from CreationModules import PriorBreathData as PDB

__author__ = 'sottilep'

# ipclient = Client()
# print(ipclient.ids)
#ipview = ipclient.direct_view()

client = MongoClient()
db = client.VentDB
input_log = db.input_log
breath_col = db.breath_collection

# input_log.drop()
# breath_col.drop()

try:
    input_log.create_index([('type', pymongo.TEXT)])
    input_log.create_index([('loaded', pymongo.ASCENDING)])
    input_log.create_index([('analyzed', pymongo.ASCENDING)])
    input_log.create_index([('loc', pymongo.GEO2D)], min = -1,
                           max = (datetime.datetime.now() + datetime.timedelta(days = 1440)).timestamp())

    breath_col.create_index([('patient_ID', pymongo.ASCENDING)])
    breath_col.create_index([('file', pymongo.TEXT)])
    breath_col.create_index([('breath_num', pymongo.ASCENDING)])
    breath_col.create_index([('date_time', pymongo.ASCENDING)])
    breath_col.create_index([('next_breath_data', pymongo.ASCENDING)])
    breath_col.create_index([('loc', pymongo.GEO2D)], min = -1,
                            max = (datetime.datetime.now() + datetime.timedelta(days = 1440)).timestamp())
except pymongo.errors.OperationFailure:
    pass


# Update List of RawDataFiles and Match Breath/Waveform Files
FS.file_search()
FS.file_match()


#@ipview.parallel(block = True)
def make_waveform_and_breath(files):
    from CreationModules import DatabaseCreation
    for file in files:
        DatabaseCreation.get_waveform_and_breath(file)


#@ipview.parallel(block = True)
def make_EHR_data(files):
    from CreationModules import EHR_decoder
    for file in files:
        EHR_decoder.load_EHR_data(file['_id'], file['patient_id'])


# Query DB for list of Waveform/breath files not yet added
files = input_log.find({'type': 'waveform', 'loaded': 0, 'errors': {'$exists': 0}, 'file_size': {'$gt': 1024}})
print('Breath: ', files.count())
make_waveform_and_breath(files)
print('finished entering breaths')

# Query DB to Add Previous Breath Data
current_breath_list = breath_col.find({'next_breath_data': {'$exists': 0}},
                                      {'patient_id': 1, 'file': 1, 'breath_num': 1, 'breath_character': 1})
print('Next Breath: ', current_breath_list.count())
PDB.update_breath(current_breath_list)
print('finished crossing next breath')

# Query DB for list of EHR files not yet added
files = input_log.find({'$or': [{'type': 'rt'}, {'type': 'rn'}, {'type': 'lab'}],
                        'loaded': 0,
                        'file_size': {'$gte': 1024}},
                       {'_id': 1, 'patient_id': 1})
print('EHR: ', files.count())
make_EHR_data(files)
print('finished EHR loading')

for items in input_log.find({'loaded': 1}, {'type': 1}):
    print(items)
