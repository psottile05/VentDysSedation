import pandas as pd
import json
import pprint
import bokeh.charts as charts


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


def analyze_max_min(max_min_df, raw_df, raw, start_time, end_insp_time, end_time, mongo_record):
    max_min_data_tot = {}

    insp_time = end_insp_time - start_time
    exp_time = end_time - end_insp_time
    elapse_time = end_time - start_time

    insp_25_time = start_time + (0.25 * insp_time)
    insp_75_time = start_time + (0.75 * insp_time)
    insp_90_time = start_time + (0.90 * insp_time)
    exp_15_time = end_insp_time + (0.15 * exp_time)
    exp_25_time = end_insp_time + (0.25 * exp_time)
    exp_75_time = end_insp_time + (0.75 * exp_time)

    max_min_df['curve'] = max_min_df['curve'].astype('category')
    grouped = max_min_df.groupby('curve')

    for curve in ['sm_flow', 'sm_paw', 'sm_vol']:
        analysis_df = grouped.get_group(curve).set_index('time')

        try:
            max_df = analysis_df.groupby('max_min').get_group(1)
            min_df = analysis_df.groupby('max_min').get_group(-1)

            max_min_data = {'n_insp_max': int(max_df['value'].loc[start_time:end_insp_time].shape[0]),
                            'n_insp_max_25': int(max_df['value'].loc[start_time:insp_25_time].shape[0]),
                            'n_insp_max_50': int(max_df['value'].loc[insp_25_time:insp_75_time].shape[0]),
                            'n_insp_max_75': int(max_df['value'].loc[insp_75_time:end_insp_time].shape[0]),
                            'n_insp_max_90': int(max_df['value'].loc[insp_90_time:end_insp_time].shape[0]),
                            'n_insp_min': int(min_df['value'].loc[start_time:end_insp_time].shape[0]),
                            'n_insp_min_25': int(min_df['value'].loc[start_time:insp_25_time].shape[0]),
                            'n_insp_min_50': int(min_df['value'].loc[insp_25_time:insp_75_time].shape[0]),
                            'n_insp_min_75': int(min_df['value'].loc[insp_75_time:end_insp_time].shape[0]),
                            'n_exp_max': int(max_df['value'].loc[end_insp_time:end_time].shape[0]),
                            'n_exp_max_15': int(max_df['value'].loc[end_insp_time:exp_15_time].shape[0]),
                            'n_exp_max_25': int(max_df['value'].loc[end_insp_time:exp_25_time].shape[0]),
                            'n_exp_max_50': int(max_df['value'].loc[exp_25_time:exp_75_time].shape[0]),
                            'n_exp_max_75': int(max_df['value'].loc[exp_75_time:end_time].shape[0])
                            }
            if max_min_data['n_insp_max'] > 0:
                max_value = float(max_df['value'].loc[start_time:end_insp_time].max())
                max_loc = int(max_df['value'].loc[start_time:end_insp_time].idxmax())
                max_min_data['insp_rise'] = float((max_value - raw_df[curve.strip('sm_')]) / (max_loc - start_time))

                if max_min_data['n_insp_max_25'] > 0:
                    max_min_data['insp_25_max'] = float(max_df['value'].loc[start_time:insp_25_time].max())
                    max_min_data['insp_rise_25'] = float((max_min_data['insp_25_max'] - raw_df[curve.strip('sm_')]) / (
                        max_df['value'].loc[start_time:insp_25_time].idxmax() - start_time))
                    max_min_data['delta_insp_max_25'] = float(max_value - max_min_data['insp_25_max'])

                if max_min_data['n_insp_max_50'] > 0:
                    max_min_data['insp_50_max'] = float(max_df['value'].loc[insp_25_time:insp_75_time].max())
                    max_min_data['insp_rise_50'] = float((max_min_data['insp_50_max'] - raw_df[curve.strip('sm_')]) / (
                        max_df['value'].loc[insp_25_time:insp_75_time].idxmax() - start_time))
                    max_min_data['delta_insp_max_50'] = float(max_value - max_min_data['insp_50_max'])

                if max_min_data['n_insp_max_75'] > 0:
                    max_min_data['insp_75_max'] = float(max_df['value'].loc[insp_75_time:end_insp_time].max())
                    max_min_data['insp_rise_75'] = float((max_min_data['insp_75_max'] - raw_df[curve.strip('sm_')]) / (
                        max_df['value'].loc[insp_75_time:end_insp_time].idxmax() - start_time))
                    max_min_data['delta_insp_max_75'] = float(max_value - max_min_data['insp_75_max'])

                if max_min_data['n_insp_max'] >= 2:
                    max_df = max_df.drop(max_df['value'].loc[start_time:end_insp_time].idxmax())

                    max_value2 = float(max_df['value'].loc[start_time:end_insp_time].max())
                    max_loc2 = int(max_df['value'].loc[start_time:end_insp_time].idxmax())

                    max_min_data['delta_insp_max'] = float(max_value2)
                    max_min_data['insp_ptp_max_delta'] = float(max_value - max_value2)
                    max_min_data['insp_ptp_time_delta'] = float(max_loc - max_loc2)
                    max_min_data['insp_ptp_rel_position'] = float(max_loc / end_insp_time)

            max_min_data_tot[curve] = max_min_data

        except KeyError as e:
            print('\t', 'Key Error: ', e, mongo_record['_id'], curve)
            # p = charts.Line(raw, x='time', y=['sm_paw', 'sm_vol', 'sm_flow'], legend='top_left')
            # charts.output_file('test.html')
            # charts.show(p)

            # print(max_min_df)

            # input('test')
            pass

    return max_min_data_tot


def analyze_breath(mongo_record):
    raw_df = pd.DataFrame(mongo_record['breath_raw'])
    raw_df = second_deriv(raw_df)
    breath_raw = raw_df.astype(float).to_dict(orient = 'list')
    mongo_record['breath_raw'] = breath_raw

    max_min_df = breath_getter(raw_df)
    max_min_raw = max_min_df[['time', 'value', 'max_min']].astype(float).to_dict(orient = 'list')
    curves = max_min_df['curve'].values.tolist()
    max_min_raw['curve'] = curves
    mongo_record['max_min_raw'] = max_min_raw

    breath_char = mongo_record['breath_character']
    max_min_data = analyze_max_min(max_min_df, raw_df[['flow', 'vol', 'paw']].iloc[0], raw_df,
                                   float(breath_char['start_time']),
                                   float(breath_char['end_insp_time']), float(breath_char['end_time']), mongo_record)
    mongo_record['max_min_analysis'] = max_min_data

    return mongo_record
