import pandas as pd


def second_deriv(raw_df):
    raw_df['sm_dF/dT'] = raw_df['sm_flow'].diff()
    raw_df['sm_dP/dT'] = raw_df['sm_paw'].diff()
    raw_df['sm_dV/dT'] = raw_df['sm_vol'].diff()

    raw_df['sm_dF/dTT'] = raw_df['sm_dF/dT'].diff()
    raw_df['sm_dP/dTT'] = raw_df['sm_dP/dT'].diff()
    raw_df['sm_dV/dTT'] = raw_df['sm_dV/dT'].diff()

    return raw_df


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
def find_max_min(name, time, curve):
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


def breath_getter(breath_df):
    for curve in ['sm_flow', 'sm_paw', 'sm_vol']:
        global last_value
        last_value = 0

        global count
        count = 0

        if curve == 'sm_flow':
            breath_df['concav'] = breath_df['sm_dF/dTT'].apply(concav)
        elif curve == 'sm_paw':
            breath_df['concav'] = breath_df['sm_dP/dTT'].apply(concav)
        elif curve == 'sm_vol':
            breath_df['concav'] = breath_df['sm_dV/dTT'].apply(concav)

        grouped = breath_df.groupby('concav')

        max_min_time = []

        for name, groups in grouped:
            result = find_max_min(name, groups['time'].values, groups[curve].values)
            if result is not None:
                max_min_time.append(result)

        max_min_time.sort(key = lambda x: x[0])

        max_min_time = clean_max_min(max_min_time)
        max_min_time = clean_max_min(max_min_time)
        max_min_time = clean_max_min(max_min_time)

    return max_min_time


def analyze_breath(mongo_record):
    raw_df = pd.DataFrame(mongo_record['breath_raw'])
    raw_df = second_deriv(raw_df)
    max_min_df = breath_getter(raw_df)
    breath_raw = raw_df.to_dict(orient = 'list')
    mongo_record['breath_raw'] = breath_raw
    return mongo_record
