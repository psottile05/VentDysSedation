__author__ = 'sottilep'

from pymongo import MongoClient
import pandas as pd
import numpy as np

pd.set_option('max_columns', 40)


def unpack_entry(data, type):
    try:
        return data[0][type]
    except KeyError:
        return np.nan


def data_collect(patient, patient_info):
    client = MongoClient()
    # print(client.database_names())

    db = client.VentDyssynchrony_db
    # print(db.collection_names())

    breath_data = db.BreathData_collection
    rn_data = db.RNData_collection
    rt_data = db.RTData_collection
    lab_data = db.LabData_collection

    results = breath_data.find({'patientID': patient},
                               {'_id': 0, 'patientID': 1, 'start_time': 1, 'analysis': 1, 'vent_settings.PEEP': 1,
                                'vent_settings.p_mean': 1, 'vent_settings.FiO2': 1})
    breath_df = pd.io.json.json_normalize(results)
    breath_df.set_index(['start_time'], inplace = True)
    breath_df['patientID'] = breath_df['patientID'].str.lstrip('P').astype(int)
    breath_df = breath_df.resample('1s', limit = 20)

    results = rn_data.find({'patientID': patient, 'RN_entry.RASS': {'$exists': 1}},
                           {'_id': 0, 'patientID': 1, 'entry_time': 1, 'RN_entry': 1})
    rn_df = pd.DataFrame.from_dict(list(results))
    if rn_df.shape[0] != 0:
        rn_df.drop_duplicates(subset = 'entry_time', keep = 'first', inplace = True)
        rn_df.set_index(['entry_time'], inplace = True, verify_integrity = True)
        rn_df['patientID'] = rn_df['patientID'].str.lstrip('P').astype(int)
        rn_df['RASS'] = rn_df['RN_entry'].apply(lambda x: unpack_entry(x, 'RASS'))
        rn_df['SpO2'] = rn_df['RN_entry'].apply(lambda x: unpack_entry(x, 'SpO2'))
        rn_df.drop(['RN_entry'], axis = 1, inplace = True)

    print(patient_info)
    if patient_info['NMB'] == 1:
        start_stop = patient_info['Start_End_NMB'].strip('[]').split('), (')
        print(start_stop)
        for items in start_stop:
            start, stop = items.strip('()').split(',')
            start = pd.to_datetime(start)
            stop = pd.to_datetime(stop)
            rn_df.loc[(rn_df.index >= start) & (rn_df.index <= stop), 'RASS'] = -6
        print(rn_df.RASS.describe())

    results = rt_data.find({'patientID': patient, 'RT_entry.Plat': {'$exists': 1}},
                           {'_id': 0, 'patientID': 1, 'entry_time': 1, 'RT_entry': 1})
    rt_df = pd.DataFrame.from_dict(list(results))
    if rt_df.shape[0] != 0:
        rt_df.drop_duplicates(subset = 'entry_time', keep = 'first', inplace = True)
        rt_df.set_index(['entry_time'], inplace = True, verify_integrity = True)
        rt_df['plat'] = rt_df['RT_entry'].apply(lambda x: unpack_entry(x, 'Plat'))
        rt_df.drop(['RT_entry', 'patientID'], axis = 1, inplace = True)

    results = lab_data.find({'patientID': patient, 'Lab_entry. ph arterial': {'$exists': 1}},
                            {'_id': 0, 'entry_time': 1, 'Lab_entry. ph arterial': 1, 'Lab_entry. po2 arterial': 1,
                             'Lab_entry. pco2 arterial': 1})
    lab_df = pd.DataFrame.from_dict(list(results))
    if lab_df.shape[0] != 0:
        lab_df['ph'] = lab_df['Lab_entry'].apply(lambda x: unpack_entry(x, ' ph arterial'))
        lab_df['paO2'] = lab_df['Lab_entry'].apply(lambda x: unpack_entry(x, ' po2 arterial'))
        lab_df['paCO2'] = lab_df['Lab_entry'].apply(lambda x: unpack_entry(x, ' pco2 arterial'))
        lab_df.drop_duplicates(subset = 'entry_time', keep = 'first', inplace = True)
        lab_df.set_index(['entry_time'], inplace = True, verify_integrity = True)
        lab_df.drop(['Lab_entry'], axis = 1, inplace = True)

    rt_df = rt_df.resample('1T', fill_method = 'bfill', limit = 120)
    lab_df = lab_df.resample('1T', fill_method = 'bfill', limit = 120)

    rn_df = rn_df.join(rt_df, how = 'left').join(lab_df, how = 'left')
    return breath_df, rn_df


def collection_freq(breath_df, win):
    for ds_type in ['ds', 'pl', 'ie']:
        breath_df['{0}_rolling'.format(ds_type)] = pd.rolling_sum(breath_df['analysis.' + ds_type], window = 60 * win,
                                                                  center = True, min_periods = 1)
        breath_df[ds_type + '_tot_rolling'] = pd.rolling_count(breath_df['analysis.' + ds_type], window = 60 * win,
                                                               center = True)
        breath_df[ds_type + '_freq'] = breath_df[ds_type + '_rolling'] / breath_df[ds_type + '_tot_rolling']

    # add rolling average for Fio2, PEEP, p_mean
    try:
        breath_df['peep_rolling'] = pd.rolling_mean(breath_df['vent_settings.PEEP'], window = 60 * win,
                                                    center = True, min_periods = 1)
    except KeyError:
        pass

    try:
        breath_df['p_mean_rolling'] = pd.rolling_mean(breath_df['vent_settings.p_mean'], window = 60 * win,
                                                      center = True, min_periods = 1)
    except KeyError:
        pass

    try:
        breath_df['fio2_rolling'] = pd.rolling_mean(breath_df['vent_settings.FiO2'], window = 60 * win,
                                                    center = True, min_periods = 1)
    except KeyError:
        pass

    return breath_df


def rolling_rass_combi(breath_df, rn_df):
    combi_df = rn_df.join(breath_df, how = 'left', lsuffix = '_l')
    combi_df.drop(
        ['analysis.ds', 'analysis.fl', 'analysis.ie', 'analysis.pl', 'analysis.pvt', 'patientID_l'],
        axis = 1, inplace = True)
    combi_df.dropna(axis = 0, how = 'all', subset = ['ds_freq', 'ie_freq', 'pl_freq'], inplace = True)

    return combi_df


def get_data(patient_list, win_range):
    total_df = pd.DataFrame()

    patient_df = pd.read_csv('C:\Research_data\Demographic Data v2.csv', engine = 'c',
                             usecols = ['Study ID', 'NMB', 'Start_End_NMB'])
    patient_df.set_index(['Study ID'], inplace = True)

    for items in patient_list:
        patient_info = patient_df.ix[int(items.lstrip('P'))]
        breath_df, rn_df = data_collect(items, patient_info)

        for win in win_range:
            breath_df = collection_freq(breath_df, win)
            combi_df = rolling_rass_combi(breath_df, rn_df)
            combi_df['patientID'].fillna(method = 'pad', inplace = True)
            combi_df['win'] = win
            total_df = pd.concat([total_df, combi_df], axis = 0)

        print(items, rn_df['RASS'].count(),
              rn_df[(rn_df.index >= breath_df.index.min()) & (rn_df.index <= breath_df.index.max())]['RASS'].count(),
              (combi_df['ds_freq'].count()))

    return total_df

# total_df = get_data(['P110'], [240])
# print(total_df)
