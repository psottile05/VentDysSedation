__author__ = 'sottilep'

from pathlib import Path
from pymongo import MongoClient, errors
import pymongo
import re
import datetime

client = MongoClient()
db = client.VentDB
input_log = db.input_log

p = Path('c:\Research_data\RawData')


def file_search():
    input_log.create_index([('type', pymongo.TEXT),
                          ('loaded', pymongo.ASCENDING),
                          ('analyzed', pymongo.ASCENDING)])
    input_log.create_index([('loc', pymongo.GEO2D)], min=-1, max=(datetime.datetime.now()+datetime.timedelta(days=1440)).timestamp())

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

            else:
                 try:
                     start_time = datetime.datetime.strptime(start_time.group(), '%y-%m-%d-%H-%M').timestamp()
                 except ValueError:
                     start_time = re.sub('-', '.', start_time.group())

            p_id = float(re.search(r'(?<=P)[0-9]*', file.as_posix()).group(0))

            try:
                input_log.insert_one({'_id': file.as_posix(),
                                      'patient_id': p_id,
                                      'type': file_type,
                                      'start_time': start_time,
                                      'loc': [start_time, p_id],
                                      'loaded': 0, 'crossed': 0, 'analyzed': 0})
            except errors.DuplicateKeyError:
                print('Dup Keys', file.name)


def file_match():
    waveform_files = input_log.find({'type': 'waveform'})
    for files in waveform_files:
        results = input_log.aggregate([{'$geoNear':
                                            {'near': files['loc'],
                                             'distanceField':'distance',
                                             'query': {'type': 'breath'},
                                             'limit': 1
                                        }}])

        for items in results:
            input_log.find_one_and_update({'_id':files['_id']},
                                          {'$set':{'match_file': items['_id'],
                                           'distance': items['distance'],
                                           'crossed': 1}})
