__author__ = 'sottilep'

from pathlib import Path
from pymongo import MongoClient, errors
import pymongo

client = MongoClient()
db = client.VentDB
input_files = db.input_log
input_files.create_index([('type', pymongo.TEXT),
                          ('loaded', pymongo.ASCENDING),
                          ('analyzed', pymongo.ASCENDING)])
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
            try:
                input_files.insert_one({'_id': file.as_posix(), 'type': file_type, 'loaded': 0, 'analyzed': 0})
            except errors.DuplicateKeyError:
                pass
