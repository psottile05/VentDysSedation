import numba
import numpy as np
import pandas as pd
from pymongo import MongoClient

client = MongoClient()
db = client.VentDB

input_log = db.input_log


# take second derivative
def second_deriv(raw_df):
    raw_df['sm_dF/dT'] = raw_df['sm_flow'].diff()
    raw_df['sm_dP/dT'] = raw_df['sm_paw'].diff()
    raw_df['sm_dV/dT'] = raw_df['sm_vol'].diff()

    raw_df['sm_dF/dTT'] = raw_df['sm_dF/dT'].diff()
    raw_df['sm_dP/dTT'] = raw_df['sm_dP/dT'].diff()
    raw_df['sm_dV/dTT'] = raw_df['sm_dV/dT'].diff()

    return raw_df


# label concave up/down
def concav(x, mongo_record):
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
        value = 0
        input_log.update_one({'_id': mongo_record['_id']},
                             {'$addToSet': {'errors': 'concave_error', 'concave_error': mongo_record['breath_num']}})

    return value


# max_min_time is a tuple (time, concav direction, value)
def find_max(name, time, curve, curve_type):
    if name > 0 and curve.shape[0] > 2:
        loc_max = curve.argmax()
        time_max = time[loc_max]
        return time_max, 1, curve[loc_max], curve_type


# clean maxes and find mins
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
                if index >= 1:
                    max_drop.append(index - 1)
            else:
                max_drop.append(index)
        elif last_index == 0:
            min_index = curve_values[last_index:curve_index].argmin() + last_index
            min_time[index] = min_index
        else:
            min_index = curve_values[last_index:curve_index].argmin() + last_index
            if abs(min_index - curve_index) > 2 and abs(min_index - last_index) > 2:
                min_time[index] = min_index
            else:
                if curve_values[curve_index] > curve_values[last_index]:
                    if index >= 1:
                        max_drop.append(index - 1)
                    if len(max_drop) > 2:
                        if max_drop[-1] == max_drop[-2]:
                            min_time[index] = curve_values[last_2_index:last_index].argmin() + last_2_index
                else:
                    max_drop.append(index)

        last_2_index = last_index
        last_index = curve_index

    return min_time, max_drop


def breath_getter(breath_df, start_time, end_insp_time, end_time, mongo_record):
    max_min_df = pd.DataFrame()
    for curve in ['sm_flow', 'sm_paw', 'sm_vol']:
        global last_value
        last_value = 0

        global count
        count = 0

        if curve == 'sm_flow':
            breath_df['concav'] = breath_df['sm_dF/dTT'].apply(concav, mongo_record = mongo_record)
            diff = 'sm_dF/dT'
        elif curve == 'sm_paw':
            breath_df['concav'] = breath_df['sm_dP/dTT'].apply(concav, mongo_record = mongo_record)
            diff = 'sm_dP/dT'
        elif curve == 'sm_vol':
            breath_df['concav'] = breath_df['sm_dV/dTT'].apply(concav, mongo_record = mongo_record)
            diff = 'sm_dV/dT'

        grouped = breath_df.groupby('concav')

        max_time = []
        for name, groups in grouped:
            result = find_max(name, groups['time'].values, groups[curve].values, curve)
            if result is not None:
                max_time.append(result)

        if len(max_time) < 1:
            max_time = [(breath_df['time'].iloc[breath_df[curve].idxmax()], 1, breath_df[curve].max(), curve)]

        max_time.sort(key = lambda x: x[0])
        max_time_df = pd.DataFrame.from_records(max_time, columns = ['time', 'max_min', 'value', 'curve'])

        time = breath_df['time'].values
        values = breath_df[curve].values
        try:
            min_time, max_drop = clean_max_min(time, values, max_time_df['time'].values)

            if min_time[0] == 0:
                min_time = min_time[np.nonzero(min_time)]
                min_time = np.insert(min_time, 0, 0)
            else:
                min_time = min_time[np.nonzero(min_time)]

            if len(min_time) < 1:
                print('no min', curve)

            min_time_value = time[min_time.astype(int)]
            min_marker = np.ones_like(min_time) * -1
            min_value = values[min_time.astype(int)]

            min_time_df = pd.DataFrame(data = {'time': min_time_value, 'max_min': min_marker, 'value': min_value})
            min_time_df['curve'] = curve

            print(min_time_df[(min_time_df.time >= start_time) & (min_time_df.time <= end_insp_time)])
            if min_time_df[(min_time_df.time >= start_time) & (min_time_df.time <= end_insp_time)].count() < 1:
                minv = breath_df[(breath_df.time >= start_time) & (breath_df.time <= end_insp_time)][curve].min()
                min_arg = breath_df[(breath_df.time >= start_time) & (breath_df.time <= end_insp_time)][curve].argmin()
                min_time_df.append(pd.DataFrame({'time': min_arg, 'max_min': -1, 'value': minv, 'curve': curve}))
            if min_time_df[(min_time_df.time >= end_insp_time) & (min_time_df.time <= end_time)].count() < 1:
                minv = breath_df[(breath_df.time >= end_insp_time) & (breath_df.time <= end_time)][curve].min()
                min_arg = breath_df[(breath_df.time >= end_insp_time) & (breath_df.time <= end_time)][curve].argmin()
                min_time_df.append(pd.DataFrame({'time': min_arg, 'max_min': -1, 'value': minv, 'curve': curve}))

            max_drop.pop(0)
            max_time_df.drop(max_drop, axis = 0, inplace = True)

            if max_time_df[(max_time_df.time >= start_time) & (max_time_df.time <= end_insp_time)].count() < 1:
                maxv = breath_df[(breath_df.time >= start_time) & (breath_df.time <= end_insp_time)][curve].max()
                max_arg = breath_df[(breath_df.time >= start_time) & (breath_df.time <= end_insp_time)][curve].argmax()
                max_time_df.append(pd.DataFrame({'time': max_arg, 'max_min': 1, 'value': maxv, 'curve': curve}))

            if max_time_df[(max_time_df.time >= end_insp_time) & (max_time_df.time <= end_time)].count() < 1:
                maxv = breath_df[(breath_df.time >= end_insp_time) & (breath_df.time <= end_time)][curve].max()
                max_arg = breath_df[(breath_df.time >= end_insp_time) & (breath_df.time <= end_time)][curve].argmax()
                max_time_df.append(pd.DataFrame({'time': max_arg, 'max_min': 1, 'value': maxv, 'curve': curve}))

            min_time_df.set_index('time', drop = False, inplace = True)
            max_time_df.set_index('time', drop = False, inplace = True)

            max_min_time_df = pd.concat([max_time_df, min_time_df]).sort_index()
            max_min_time_df.drop_duplicates(subset = 'time', keep = 'first', inplac = True)

        except Exception as e:
            print('Error with max/min clean', e)
            max_min_time_df = pd.DataFrame()
            input_log.update_one({'_id': mongo_record['_id']}, {
                '$addToSet': {'errors': 'max_min_error', 'max_min_error': mongo_record['breath_num']}})

        max_min_df = pd.concat([max_min_df, max_min_time_df])

    return max_min_df


def analyze_max_min(max_min_df, raw_df, curve_df, start_time, end_insp_time, end_time, mongo_record):
    max_min_data_tot = {}

    insp_time = end_insp_time - start_time
    exp_time = end_time - end_insp_time

    insp_25_time = start_time + (0.25 * insp_time)
    insp_75_time = start_time + (0.75 * insp_time)
    insp_90_time = start_time + (0.90 * insp_time)
    exp_15_time = end_insp_time + (0.15 * exp_time)
    exp_25_time = end_insp_time + (0.25 * exp_time)
    exp_75_time = end_insp_time + (0.75 * exp_time)

    max_min_df['curve'] = max_min_df['curve'].astype('category')
    grouped = max_min_df.groupby('curve')

    for curve in ['sm_flow', 'sm_paw', 'sm_vol']:
        if curve == 'sm_flow':
            diff = 'sm_dF/dT'
        elif curve == 'sm_paw':
            diff = 'sm_dP/dT'
        elif curve == 'sm_vol':
            diff = 'sm_dV/dT'
        else:
            diff = 'sm_dF/dT'

        max = curve_df[curve].max() * 0.75
        shoulder = curve_df[(curve_df[diff] < .75) & (curve_df[curve] > max)].head(1)
        if shoulder.shape[0] != 0:
            shoulder_time = int(shoulder['time'].iloc[0])
            shoulder_amp = float(shoulder[curve].iloc[0])
            if insp_time != 0:
                shoulder_time_percent = (end_insp_time - shoulder_time) / insp_time
            else:
                shoulder_time_percent = np.nan
        else:
            shoulder_time = np.nan
            shoulder_amp = np.nan
            shoulder_time_percent = np.nan

        try:
            analysis_df = grouped.get_group(curve).set_index('time')

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
                            'n_exp_max_75': int(max_df['value'].loc[exp_75_time:end_time].shape[0]),
                            'shoulder_time': shoulder_time,
                            'shoulder_amp': shoulder_amp,
                            'shoulder_time_percent': shoulder_time_percent,
                            'delta_insp_max': 0,
                            'delta_insp_max_25': 0,
                            'delta_insp_max_50': 0,
                            'delta_insp_max_75': 0,
                            'insp_25_max': 0,
                            'insp_50_max': 0,
                            'insp_75_max': 0,
                            'insp_ptp_max_delta': 0,
                            'insp_ptp_rel_position': 0,
                            'insp_ptp_time_delta': 0,
                            'insp_rise': 0,
                            'insp_rise_25': 0,
                            'insp_rise_50': 0,
                            'insp_rise_75': 0,
                            }

            if max_min_data['n_insp_max'] > 0:
                max_value = float(max_df['value'].loc[start_time:end_insp_time].max())
                max_loc = int(max_df['value'].loc[start_time:end_insp_time].idxmax())
                max_min_data['insp_rise'] = float(
                    (max_value - raw_df[curve.strip('sm_')].min()) / (max_loc - start_time))
                max_min_data['delta_insp_max'] = float(max_value - raw_df[curve.strip('sm_')].min())

                if max_min_data['n_insp_max_25'] > 0:
                    max_min_data['insp_25_max'] = float(max_df['value'].loc[start_time:insp_25_time].max())
                    max_min_data['insp_rise_25'] = float(
                        (max_min_data['insp_25_max'] - raw_df[curve.strip('sm_')].min()) / (
                        max_df['value'].loc[start_time:insp_25_time].idxmax() - start_time))
                    max_min_data['delta_insp_max_25'] = float(max_value - max_min_data['insp_25_max'])

                if max_min_data['n_insp_max_50'] > 0:
                    max_min_data['insp_50_max'] = float(max_df['value'].loc[insp_25_time:insp_75_time].max())
                    max_min_data['insp_rise_50'] = float(
                        (max_min_data['insp_50_max'] - raw_df[curve.strip('sm_')].min()) / (
                        max_df['value'].loc[insp_25_time:insp_75_time].idxmax() - start_time))
                    max_min_data['delta_insp_max_50'] = float(max_value - max_min_data['insp_50_max'])

                if max_min_data['n_insp_max_75'] > 0:
                    max_min_data['insp_75_max'] = float(max_df['value'].loc[insp_75_time:end_insp_time].max())
                    max_min_data['insp_rise_75'] = float(
                        (max_min_data['insp_75_max'] - raw_df[curve.strip('sm_')].min()) / (
                        max_df['value'].loc[insp_75_time:end_insp_time].idxmax() - start_time))
                    max_min_data['delta_insp_max_75'] = float(max_value - max_min_data['insp_75_max'])

                if max_min_data['n_insp_max'] >= 2:
                    max_df = max_df.drop(max_df['value'].loc[start_time:end_insp_time].idxmax())

                    max_value2 = float(max_df['value'].loc[start_time:end_insp_time].max())
                    max_loc2 = int(max_df['value'].loc[start_time:end_insp_time].idxmax())

                    max_min_data['insp_ptp_max_delta'] = float(max_value - max_value2)
                    max_min_data['insp_ptp_time_delta'] = float(max_loc2 - max_loc)
                    max_min_data['insp_ptp_rel_position'] = float((end_insp_time - max_loc) / end_insp_time)

            max_min_data_tot[curve] = max_min_data

        except KeyError as e:
            print('\t', 'Key Error: ', e, mongo_record['_id'], curve)
            print('\t', mongo_record['max_min_raw'])
            input_log.update_one({'_id': mongo_record['_id']}, {
                '$addToSet': {'errors': 'WA_analysis_key_error', 'WA_analysis_key_error': mongo_record['breath_num']}})
        except Exception as e:
            input_log.update_one({'_id': mongo_record['_id']}, {
                '$addToSet': {'errors': 'WA_analysis_error',
                              'WA_analysis' + str(e) + '_error': mongo_record['breath_num']}})
    return max_min_data_tot


def analyze_breath(mongo_record):
    raw_df = pd.DataFrame(mongo_record['breath_raw'])
    raw_df = second_deriv(raw_df)
    breath_raw = raw_df.astype(float).to_dict(orient = 'list')
    mongo_record['breath_raw'] = breath_raw

    breath_char = mongo_record['breath_character']
    max_min_df = breath_getter(raw_df, float(breath_char['start_time']),
                               float(breath_char['end_insp_time']), float(breath_char['end_time']), mongo_record)
    max_min_raw = max_min_df[['time', 'value', 'max_min']].astype(float).to_dict(orient = 'list')
    curves = max_min_df['curve'].values.tolist()
    max_min_raw['curve'] = curves
    mongo_record['max_min_raw'] = max_min_raw

    max_min_data = analyze_max_min(max_min_df, raw_df[['flow', 'vol', 'paw']].iloc[0], raw_df,
                                   float(breath_char['start_time']),
                                   float(breath_char['end_insp_time']), float(breath_char['end_time']), mongo_record)
    mongo_record['max_min_analysis'] = max_min_data

    return mongo_record
