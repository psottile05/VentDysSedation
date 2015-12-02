import numpy as np
import pandas as pd
from ggplot import *
from pymongo import MongoClient

client = MongoClient()
db = client.VentDB
breath_db = db.breath_collection


def breath_viz(id):
    breath = list(breath_db.find({'_id': id}, {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]

    breath_start = breath['breath_raw']['time'][0]
    breath_end = breath['breath_raw']['time'][-1]

    pre_breath = list(breath_db.find({'file': breath['file'], 'breath_num': breath['breath_num'] - 1},
                                     {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]
    post_breath = list(breath_db.find({'file': breath['file'], 'breath_num': breath['breath_num'] + 1},
                                      {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]

    df = pd.concat([pd.DataFrame(pre_breath['breath_raw']), pd.DataFrame(breath['breath_raw']),
                    pd.DataFrame(post_breath['breath_raw'])])

    p = ggplot(aes(x = 'time', y = 'sm_flow'), data = df) + geom_line()
    p = p + geom_vline(xintercept = [breath_start, breath_end], color = 'blue')
    print(p)


results = breath_db.find({'patient_id': 100, 'breath_num': 1})

for items in list(results):
    print(items.keys())
    breath_viz(items['_id'])
