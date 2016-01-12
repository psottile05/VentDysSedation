import numpy as np
import pandas as pd
from pymongo import MongoClient
from ggplot import *
import numba

client = MongoClient()
db = client.VentDB
breath_db = db.breath_collection


def find_shoulder(curve_df, curve, diff):
    print(curve_df.head(10))
    max = curve_df[curve].max() * 0.75
    shoulder_time = curve_df[(curve_df[diff] < .75) & (curve_df[curve] > max)].head(1)['time'].values.tolist()
    print(shoulder_time)
    return shoulder_time

def breath_getter(id):
    global last_value
    last_value = 0

    global count
    count = 0

    breath = list(breath_db.find({'_id': id}, {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]
    breath_df = pd.DataFrame(breath['breath_raw'])
    print(breath_df.columns)
    max_time = []
    curve = 'sm_vol'
    if curve == 'sm_flow':
        diff = 'sm_dF/dT'
    elif curve == 'sm_paw':
        diff = 'sm_dP/dT'
    elif curve == 'sm_vol':
        diff = 'sm_dV/dT'

    shoulder = find_shoulder(breath_df[['time', curve, diff]], curve, diff)

    p = ggplot(aes(x = 'time', y = curve), data = breath_df) + geom_line()
    p = p + geom_vline(xintercept = shoulder, color = 'blue')
    print(p)


results = breath_db.find().skip(1).limit(30)
for items in list(results):
    print(items['_id'])
    breath_getter(items['_id'])
