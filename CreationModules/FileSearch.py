__author__ = 'sottilep'

from pathlib import Path
from pymongo import MongoClient, errors
import pymongo
import re
import datetime

client = MongoClient()
db = client.VentDB
input_log = db.input_log
input_log.create_index([('type', pymongo.TEXT),
                          ('loaded', pymongo.ASCENDING),
                          ('analyzed', pymongo.ASCENDING),
                          ])
p = Path('c:\Research_data\RawData')


def file_search():
    for x in p.iterdir():
        files = [y for y in x.glob('*.txt')]
        for file in files:
            if 'Breath' in file.name or 'breath' in file.name:
                file_type = "breath"
            if 'Waveform' in file.name or 'waveform' in file.name:
                file_type = "waveform"
            if 'RT' in file.name or 'rt' in file.name:
                file_type = "rt"
            if 'RN' in file.name or 'rn' in file.name:
                file_type = "rt"
            if 'Lab' in file.name or 'lab' in file.name:
                file_type = "lab"

            start_time = re.search(r'(\d\d-\d\d-\d\d-\d\d-\d\d)|(\d\d\d-\d)', file.name)

            if isinstance(start_time, type(None)):
                start_time = -1
                print(file)
            else:
                 try:
                     start_time = datetime.datetime.strptime(start_time.group(), '%y-%m-%d-%H-%M').timestamp()
                 except ValueError:
                     start_time = re.sub('-', '.', start_time.group())

            try:
                input_log.insert_one({'_id': file.as_posix(),
                                      'patient_id': re.search(r'(?<=/P)[0-9]*', file.as_posix()).group(),
                                      'type': file_type,
                                      'start_time': start_time,
                                      'loc': [start_time, 0],
                                      'match_file':'',
                                      'loaded': 0, 'crossed': 0, 'analyzed': 0})
            except errors.DuplicateKeyError:
                print('Dup Keys', file.name)


def file_match():
    results = input_log.aggregate()

'''
result = input_log.aggregate([{'$group': {
                                '_id': '$start_time',
                                'file_name': {'$addToSet':'$patient_id'},
                                'file_type': {'$addToSet':'$type'}}},
                              {'$project': {
                                '_id': 1,
                                'file_name': 1,
                                'file_type': 1,
                                'tot': {'$size': '$file_type'}}},
                              {'$match': {'tot': {'$lt':2}}},
                              {'$sort': {'file_name':1}}
                            ])

for items in result:
    print(items)
    '''