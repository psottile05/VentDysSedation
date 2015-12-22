import numpy as np
import pandas as pd
from pymongo import MongoClient
from ggplot import *

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
def find_max(name, time, curve):
    if name > 0 and curve.shape[0] > 2:
        loc_max = curve.argmax()
        time_max = time[loc_max]
        return (time_max, 1, curve[loc_max])


def clean_max_min(time, curve_values, max_time):
    end = len(max_time) - 1
    last_index = 0
    min_time = np.zeros(len(max_time) + 1)
    max_drop = []

    for index, mtime in np.ndenumerate(max_time):
        index = index[0]
        curve_index = int(np.where(time == mtime)[0])
        print(index, curve_index, last_index)

        if abs(curve_index - last_index) < 4:
            if curve_values[curve_index] > curve_values[last_index]:
                if index >= 1: max_drop.append(index - 1)
            else:
                max_drop.append(index)
        else:
            min_index = curve_values[last_index:curve_index].argmin() + last_index
            if (abs(min_index - curve_index) > 1 and abs(min_index - last_index) > 1) or last_index == 0:
                if (curve_values[last_index] - curve_values[min_index]) / curve_values[last_index] < 0.99:
                    if (curve_values[curve_index] - curve_values[min_index]) / curve_values[curve_index] < 0.99:
                        min_time[index] = min_index
                else:
                    if curve_values[curve_index] > curve_values[last_index]:
                        if index >= 1: max_drop.append(index - 1)
                    else:
                        max_drop.append(index)
            else:
                if curve_values[curve_index] > curve_values[last_index]:
                    if index >= 1: max_drop.append(index - 1)
                else:
                    max_drop.append(index)

        last_index = curve_index

    min_time = min_time[np.nonzero(min_time)]
    min_time = time[min_time.astype(int)]

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

    min_time, max_drop = clean_max_min(breath_df['time'].values, breath_df[curve].values, max_time_df['time'].values)
    max_time_df.drop(max_drop, axis = 0, inplace = True)

    p = ggplot(aes(x = 'time', y = 'sm_flow'), data = breath_df) + geom_line()
    p = p + geom_vline(xintercept = min_time, color = 'blue')
    p = p + geom_vline(xintercept = max_time_df['time'], color = 'red')
    print(p)


results = breath_db.find().limit(1000)
for items in list(results):
    breath_getter(items['_id'])
