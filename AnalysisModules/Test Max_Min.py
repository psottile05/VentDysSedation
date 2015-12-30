import numpy as np
import pandas as pd
from pymongo import MongoClient
from ggplot import *
import numba

client = MongoClient()
db = client.VentDB
breath_db = db.breath_collection


def concav(x):
    global last_value
    global count

    try:
        if x <= 0:
            if last_value == 0:
                count += 1
            value = count
            last_value = 1
        else:
            last_value = 0
            value = -count
    except TypeError:
        print(x)

    return value

# max_min_time is a tuple (time, concav direction, value)
@numba.jit(nopython = True, nogil = True, cache = True)
def find_max(name, time, curve):
    if name > 0 and curve.shape[0] > 2:
        loc_max = curve.argmax()
        time_max = time[loc_max]
        return (time_max, 1, curve[loc_max])


@numba.jit(nopython = True, nogil = True, cache = True)
def clean_max_min(time, curve_values, max_time):
    last_index = 0
    last_2_index = 0
    min_time = np.zeros(len(max_time) + 1)
    max_drop = [0]

    for index_t, mtime in np.ndenumerate(max_time):
        index = index_t[0]
        curve_index = int(np.where(time == mtime)[0][0])

        if abs(curve_index - last_index) < 4 and last_index != 0:
            if curve_values[curve_index] > curve_values[last_index]:
                if index >= 1: max_drop.append(index - 1)
            else:
                max_drop.append(index)
        elif last_index == 0:
            min_index = curve_values[last_index:curve_index].argmin() + last_index
            min_time[index] = min_index
        else:
            min_index = curve_values[last_index:curve_index].argmin() + last_index
            if (abs(min_index - curve_index) > 2 and abs(min_index - last_index) > 2):
                min_time[index] = min_index
            else:
                if curve_values[curve_index] > curve_values[last_index]:
                    if index >= 1: max_drop.append(index - 1)
                    if len(max_drop) > 2:
                        if max_drop[-1] == max_drop[-2]:
                            min_time[index] = curve_values[last_2_index:last_index].argmin() + last_2_index
                else:
                    max_drop.append(index)

        last_2_index = last_index
        last_index = curve_index

    return min_time, max_drop


def breath_getter(id):
    global last_value
    last_value = 0

    global count
    count = 0

    breath = list(breath_db.find({'_id': id}, {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]
    breath_df = pd.DataFrame(breath['breath_raw'])

    breath_df['concav'] = breath_df['sm_dF/dTT'].apply(concav)
    grouped = breath_df.groupby('concav')

    max_time = []
    curve = 'sm_flow'

    for name, groups in grouped:
        result = find_max(name, groups['time'].values, groups[curve].values)
        if result is not None:
            max_time.append(result)

    max_time.sort(key = lambda x: x[0])
    max_time_df = pd.DataFrame.from_records(max_time, columns = ['time', 'max', 'value'])

    time = breath_df['time'].values
    try:
        min_time, max_drop = clean_max_min(time, breath_df[curve].values, max_time_df['time'].values)
    except:
        print(time, breath_df[curve].values, max_time_df['time'].values)
        p = ggplot(aes(x = 'time', y = 'sm_flow'), data = breath_df) + geom_line()
        print(p)

    if min_time[0] == 0:
        min_time = min_time[np.nonzero(min_time)]
        min_time = np.insert(min_time, 0, 0)
    else:
        min_time = min_time[np.nonzero(min_time)]

    min_time = time[min_time.astype(int)]
    max_drop.pop(0)
    max_time_df.drop(max_drop, axis = 0, inplace = True)

    # p = ggplot(aes(x = 'time', y = 'sm_flow'), data = breath_df) + geom_line()
    # p = p + geom_vline(xintercept = min_time, color = 'blue')
    # p = p + geom_vline(xintercept = max_time_df['time'], color = 'red')
    # print(p)


results = breath_db.find().limit(1000)
for items in list(results):
    print(items['_id'])
    breath_getter(items['_id'])
