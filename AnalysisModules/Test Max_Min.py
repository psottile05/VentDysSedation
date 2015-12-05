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
def find_max_min(name, time, curve, max_min_time):
    if name > 0 and curve.shape[0] > 2:
        loc_max = curve.argmax()
        time_max = time[loc_max]
        return (time_max, 1, curve[loc_max])

    elif name <= 0 and curve.shape[0] > 2:
        loc_min = curve.argmin()
        time_min = time[loc_min]
        return (time_min, -1, curve[loc_min])

def clean_max_min(max_min_time):
    for index, items in enumerate(max_min_time):
        try:
            if index == 0:
                pass
            elif -items[1] == max_min_time[index + 1][1] and -items[1] == max_min_time[index - 1][1]:
                if max_min_time[index + 1][0] - items[0] < 128:
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


def breath_getter(id):
    global last_value
    last_value = 0

    global count
    count = 0

    breath = list(breath_db.find({'_id': id}, {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]
    breath_df = pd.DataFrame(breath['breath_raw'])

    breath_df['concav'] = breath_df['sm_dF/dTT'].apply(concav)
    grouped = breath_df.groupby('concav')

    max_min_time = []
    curve = 'sm_flow'

    for name, groups in grouped:
        result = find_max_min(name, groups['time'].values, groups[curve].values, max_min_time)
        if result is not None:
            max_min_time.append(result)

    max_min_time.sort(key = lambda x: x[0])

    max_min_time = clean_max_min(max_min_time)
    max_min_time = clean_max_min(max_min_time)
    max_min_time = clean_max_min(max_min_time)


def print_plot(breath_df, max_min_time):
    max_time = []
    min_time = []

    for items in max_min_time:
        if items[1] == 1:
            max_time.append(items[0])
        else:
            min_time.append(items[0])

    p = ggplot(aes(x = 'time', y = 'sm_flow'), data = breath_df) + geom_line()
    p = p + geom_vline(xintercept = max_time, color = 'green')
    p = p + geom_vline(xintercept = min_time, color = 'red')
    print(p)


results = breath_db.find().limit(100)
for items in list(results):
    breath_getter(items['_id'])
