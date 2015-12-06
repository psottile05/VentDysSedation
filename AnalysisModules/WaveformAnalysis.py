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
def find_max_min(name, time, curve, curve_type):
    if name > 0 and curve.shape[0] > 2:
        loc_max = curve.argmax()
        time_max = time[loc_max]
        return time_max, 1, curve[loc_max], curve_type

    elif name <= 0 and curve.shape[0] > 2:
        loc_min = curve.argmin()
        time_min = time[loc_min]
        return time_min, -1, curve[loc_min], curve_type


def clean_max_min(max_min_time):
    for index, items in enumerate(max_min_time):
        try:
            if index == 0:
                pass
            elif -items[1] == max_min_time[index + 1][1] and -items[1] == max_min_time[index - 1][1]:
                if max_min_time[index + 1][0] - items[0] < 128:
                    max_min_time.pop(index + 1)
                    max_min_time.pop(index)
                elif max_min_time[index - 1][2] < items[2] < max_min_time[index + 1][2] or max_min_time[index - 1][2] \
                        > items[2] > max_min_time[index + 1][2]:
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
    max_min_df = pd.DataFrame()
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
            result = find_max_min(name, groups['time'].values, groups[curve].values, curve)
            if result is not None:
                max_min_time.append(result)

        max_min_time.sort(key = lambda x: x[0])

        max_min_time = clean_max_min(max_min_time)
        max_min_time = clean_max_min(max_min_time)
        max_min_time = clean_max_min(max_min_time)

        max_min_temp_df = pd.DataFrame.from_records(max_min_time, columns = ['time', 'max_min', 'value', 'curve'])
        max_min_df = pd.concat([max_min_temp_df, max_min_df])

    return max_min_df


def analyze_max_min(max_min_df, raw_df, start_time, end_insp_time, end_time):
    insp_time = end_insp_time - start_time
    exp_time = end_time - end_insp_time
    elapse_time = end_time - start_time

    insp_25_time = start_time + (0.25 * insp_time)
    insp_75_time = start_time + (0.75 * insp_time)
    insp_90_time = start_time + (0.90 * insp_time)
    exp_25_time = end_insp_time + (0.25 * exp_time)
    exp_75_time = end_insp_time + (0.75 * exp_time)

    max_min_df['curve'] = max_min_df['curve'].astype('category')
    grouped = max_min_df.groupby('curve')

    for curve in ['sm_flow', 'sm_paw', 'sm_vol']:
        analysis_df = grouped.get_group(curve).set_index('time')
        max_df = analysis_df.groupby('max_min').get_group(1)
        min_df = analysis_df.groupby('max_min').get_group(-1)


        '''
        max_min_data = {'n_insp_max': max_df['value'].loc[start_time:end_insp_time].shape[0]
                        'n_insp_max_25': max_df['value'].loc[start_time:insp_25_time].shape[0]
                        'n_insp_max_50': max_df['value'].loc[insp_25_time:insp_75_time].shape[0]
                        'n_insp_max_75': max_df['value'].loc[insp_75_time:end_insp_time].shape[0]
                        'n_insp_max_90': max_df['value'].loc[insp_90_time:end_insp_time].shape[0]
                        'insp_rise':
                        'insp_rise_25':
                        'insp_rise_50':
                        'insp_rise_75':
                        'delta_insp_max':
                        'delta_insp_max_25':
                        'delta_insp_max_50':
                        'delta_insp_max_75':
                        'n_insp_min':
                        'n_insp_min_25':
                        'n_insp_min_50':
                        'n_insp_min_75':
                        'insp_ptp_max_delta':
                        'insp_ptp_min_delta':
                        'insp_ptp_time_delta':
                        'insp_ptp_rel_position':

                        'n_exp_max':
                        'n_exp_max_25':
                        'n_exp_max_50':
                        'n_exp_max_75:'}
        max_min_data_tot = {curve:max_min_data}
        '''
        # return max_min_data_tot


def analyze_breath(mongo_record):
    raw_df = pd.DataFrame(mongo_record['breath_raw'])
    raw_df = second_deriv(raw_df)
    breath_raw = raw_df.to_dict(orient = 'list')
    mongo_record['breath_raw'] = breath_raw

    max_min_df = breath_getter(raw_df)
    max_min_raw = max_min_df.to_dict(orient = 'list')
    mongo_record['max_min_raw'] = max_min_raw

    breath_char = mongo_record['breath_character']
    max_min_data = analyze_max_min(max_min_df, raw_df[['flow', 'vol', 'paw']].iloc[0], float(breath_char['start_time']),
                                   float(breath_char['end_insp_time']), float(breath_char['end_time']))

    # breath_char.update(max_min_data)
    mongo_record['breath_caracter'] = breath_char

    return mongo_record
