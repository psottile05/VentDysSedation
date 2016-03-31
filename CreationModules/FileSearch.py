import datetime
import os
import re
from pathlib import Path

from pymongo import MongoClient, errors

__author__ = 'sottilep'

client = MongoClient()
db = client.VentDB
input_log = db.input_log

if os.name == 'nt':
    p = Path('c:\Research_data\RawData')
elif os.name == 'posix':
    p = Path('/media/veracrypt1/Research_Data/VentDysSedation/RawData/')


def match_stats():
    for output in input_log.aggregate([{'$match': {'crossed': 1}},
                                       {'$group': {
                                           '_id': '$patient_id',
                                           'min_dist': {'$min': '$distance'},
                                           'max_dist': {'$max': '$distance'},
                                           'avg_dist': {'$avg': '$distance'},
                                           'count': {'$sum': 1}}},
                                       {'$sort': {'_id': 1}}
                                       ]):
        print(output)
    print(input_log.find({'type': 'waveform'}).count(), input_log.find({'crossed': 1}).count())


def file_search():
    for x in p.iterdir():
        files = [y for y in x.glob('*.txt')]
        for file in files:
            if 'Breath' in file.name or 'breath' in file.name:
                file_type = "breath"
            elif 'Waveform' in file.name or 'waveform' in file.name:
                file_type = "waveform"
            elif ('RT' in file.name or 'rt' in file.name) and ('edit' not in file.name):
                file_type = "rt"
            elif ('RN' in file.name or 'rn' in file.name) and ('edit' not in file.name):
                file_type = "rn"
            elif ('Lab' in file.name or 'lab' in file.name) and ('edit' not in file.name):
                file_type = "lab"
            else:
                file_type = 'other'

            start_time = re.search(r'(\d\d-\d\d-\d\d-\d\d-\d\d)|(\d\d\d-\d)', file.name)

            if isinstance(start_time, type(None)):
                start_time = datetime.datetime(1970, 1, 1)

            else:
                try:
                    start_time = datetime.datetime.strptime(start_time.group(), '%y-%m-%d-%H-%M')
                except ValueError:
                    print('ValueError', file.name)

            p_id = float(re.search(r'(?<=P)[0-9]*', file.as_posix()).group(0))

            if os.name == 'nt':
                nt = str(file)
                posix = ''
            elif os.name == 'posix':
                posix = file.as_posix()
                nt = ''

            if file.stat().st_size > 100:
                try:
                    input_log.insert_one({'_id': str(p_id) + '_' + file.name,
                                          'patient_id': int(p_id),
                                          'type': file_type,
                                          'file_name': {'posix': [posix], 'nt': [nt]},
                                          'file_size': file.stat().st_size,
                                          'start_time': start_time,
                                          'loc': [start_time.timestamp(), p_id],
                                          'loaded': 0, 'crossed': 0})
                except errors.DuplicateKeyError:
                    input_log.update_one({'_id': str(p_id) + '_' + file.name},
                                         {'$addToSet': {'file_name.posix': posix, 'file_name.nt': nt}})
                except Exception as e:
                    print('File Error: ', e)


def file_match():
    waveform_files = input_log.find({'type': 'waveform', 'crossed': 0})
    for files in waveform_files:
        results = input_log.aggregate([{'$geoNear':
                                            {'near': files['loc'],
                                             'distanceField': 'distance',
                                             'query': {'type': 'breath', 'patient_id': files['patient_id']},
                                             'maxDistance': 100,
                                             'limit': 1
                                             }}])

        for items in results:
            input_log.find_one_and_update({'_id': files['_id']},
                                          {'$set': {'match_file': items['_id'],
                                                    'distance': items['distance'],
                                                    'crossed': 1}})

            # To Assess Distances Matches
            # match_stats()
