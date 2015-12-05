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

    try:
        pre_breath = list(breath_db.find({'file': breath['file'], 'breath_num': breath['breath_num'] - 1},
                                         {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]
    except IndexError:
        pre_breath = {
            'breath_raw': {'flow': [], 'sm_dV/dTT': [], 'vol': [], 'dF/dT': [], 'sm_vol': [], 'breath': [], 'time': [],
                           'sm_dF/dT': [], 'sm_flow': [], 'status': [], 'sm_dF/dTT': [], 'sm_dV/dT': [], 'dP/dV': [],
                           'paw': [], 'sm_paw': [], 'sm_dP/dT': [], 'dF/dV': [], 'dP/dT': [], 'dV/dT': [], 'dF/dP': [],
                           'sm_dP/dTT': []}}

    try:
        post_breath = list(breath_db.find({'file': breath['file'], 'breath_num': breath['breath_num'] + 1},
                                          {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]
    except IndexError:
        post_breath = {
            'breath_raw': {'flow': [], 'sm_dV/dTT': [], 'vol': [], 'dF/dT': [], 'sm_vol': [], 'breath': [], 'time': [],
                           'sm_dF/dT': [], 'sm_flow': [], 'status': [], 'sm_dF/dTT': [], 'sm_dV/dT': [], 'dP/dV': [],
                           'paw': [], 'sm_paw': [], 'sm_dP/dT': [], 'dF/dV': [], 'dP/dT': [], 'dV/dT': [], 'dF/dP': [],
                           'sm_dP/dTT': []}}

    df = pd.concat([pd.DataFrame(pre_breath['breath_raw']), pd.DataFrame(breath['breath_raw']),
                    pd.DataFrame(post_breath['breath_raw'])])

    def concav(x):
        global last_value
        global count

        if x <= 0:
            if last_value == 0:
                count += 1
            value = count
            last_value = 1
        else:
            last_value = 0
            value = -count

        return value

    global last_value
    last_value = 0

    global count
    count = 0

    breath_df = pd.DataFrame(breath['breath_raw'])
    breath_df['concav'] = breath_df['sm_dF/dTT'].apply(concav)
    grouped = breath_df.groupby('concav')

    max_time = []
    max_value = []
    min_time = []
    min_value = []

    for name, groups in grouped:
        if name > 0 and groups.shape[0] > 2:
            loc_max = groups['sm_flow'].idxmax()
            time = groups['time'].at[loc_max]
            if len(max_time) == 0 or time - max_time[-1] > 248:
                max_time.append(time)
                max_value.append(groups['sm_flow'].at[loc_max])
            elif groups['sm_flow'].at[loc_max] > max_value[-1]:
                max_time[-1] = time
                max_value[-1] = groups['sm_flow'].at[loc_max]

        if name <= 0 and groups.shape[0] > 2:
            loc_min = groups['sm_flow'].idxmin()
            time = groups['time'].at[loc_min]
            if len(min_time) == 0 or np.abs(time - min_time[-1]) > 248:
                min_time.append(time)
                min_value.append(groups['sm_flow'].at[loc_min])
            elif groups['sm_flow'].at[loc_min] < min_value[-1]:
                min_time[-1] = time
                min_value[-1] = groups['sm_flow'].at[loc_min]

    min_time.reverse()
    min_value.reverse()

    max_time = [(x, 1, y) for x, y in zip(max_time, max_value)]
    min_time = [(x, -1, y) for x, y in zip(min_time, min_value)]

    max_min_time = max_time + min_time
    max_min_time.sort(key = lambda x: x[0])

    def clean_max_min(max_min_time):
        for index, items in enumerate(max_min_time):
            try:
                if index == 0:
                    pass
                elif -items[1] == max_min_time[index + 1][1] and -items[1] == max_min_time[index - 1][1]:
                    if max_min_time[index + 1][0] - items[0] < 64:
                        max_min_time.pop(index + 1)
                        max_min_time.pop(index)
                    elif max_min_time[index - 1][2] < items[2] < max_min_time[index + 1][2] or max_min_time[index - 1][
                        2] > items[2] > max_min_time[index + 1][2]:
                        max_min_time.pop(index)
                elif -items[1] != max_min_time[index + 1][1]:
                    if items[1] == 1:
                        if items[2] > max_min_time[index + 1][2]:
                            max_min_time.pop(index + 1)
                        elif items[2] < max_min_time[index + 1][2]:
                            max_min_time.pop(index)
                    else:
                        if items[2] > max_min_time[index + 1][2]:
                            max_min_time.pop(index)
                        elif items[2] < max_min_time[index + 1][2]:
                            max_min_time.pop(index + 1)
                elif -items[1] != max_min_time[index - 1][1]:
                    if items[1] == 1:
                        if items[2] > max_min_time[index - 1][2]:
                            max_min_time.pop(index - 1)
                        elif items[2] < max_min_time[index - 1][2]:
                            max_min_time.pop(index)
                    else:
                        if items[2] > max_min_time[index - 1][2]:
                            max_min_time.pop(index)
                        elif items[2] < max_min_time[index - 1][2]:
                            max_min_time.pop(index - 1)
                elif items[1] == 1 and items[2] < max_min_time[index + 1][2]:
                    max_min_time.pop(index)
                else:
                    print(index, items, max_min_time[index + 1])
            except IndexError:
                pass

        return max_min_time

    max_min_time = clean_max_min(max_min_time)
    max_min_time = clean_max_min(max_min_time)

    max_time = []
    min_time = []

    for items in max_min_time:
        if items[1] == 1:
            max_time.append(items[0])
        else:
            min_time.append(items[0])

    p = ggplot(aes(x = 'time', y = 'sm_flow'), data = df) + geom_line()
    p = p + geom_vline(xintercept = [breath_start, breath_end], color = 'blue')
    p = p + geom_vline(xintercept = max_time, color = 'green')
    p = p + geom_vline(xintercept = min_time, color = 'red')
    print(p)


results = breath_db.find().limit(50)
for items in list(results):
    print(items['_id'])
    breath_viz(items['_id'])
