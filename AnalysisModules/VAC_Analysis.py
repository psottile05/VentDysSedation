import pymongo
import pandas as pd
import numpy as np

client = pymongo.MongoClient()
db = client.VentDyssynchrony_db
ventSettings = db.VentSettings_collection
breathData = db.BreathData_collection
RT = db.RTData_collection
RN = db.RNData_collection


def custom_resampler(dict_like):
    return dict_like['analysis.' + ds_types].sum(), dict_like['breath_number'].count(), dict_like['patientID'].max(), \
           dict_like['vent_settings.FiO2'].mean(), dict_like['vent_settings.PEEP'].mean(), \
           dict_like['vent_settings.compliance'].mean(), dict_like['vent_settings.p_peak'].mean(), \
           dict_like['vent_settings.set_VT'].mean()


def bin_samples(x):
    if x <= 0.05:
        return 0
    elif x == np.nan:
        return np.nan
    elif x > 0.05:
        return 1


def unpack(x):
    fio2 = np.nan
    set_vt = np.nan
    peep = np.nan

    for items in x:
        for keys, values in items.items():
            if keys == 'FiO2':
                fio2 = values
            elif keys == 'Set Vt':
                set_vt = values
            elif keys == 'PEEP':
                peep = values

    return fio2, peep, set_vt


for ds_types in ['ds', 'pl', 'pvt', 'ie']:
    data = breathData.find({},
                           {'patientID': 1, 'start_time': 1, 'breath_number': 1, 'vent_settings.PEEP': 1,
                            'vent_settings.FiO2': 1, 'vent_settings.compliance': 1, 'vent_settings.set_VT': 1,
                            'vent_settings.p_peak': 1, 'analysis.' + ds_types: 1, '_id': 0})

    rn = RN.find({'RN_entry.FiO2': {'$exists': 1}},
                 {'patientID': 1, 'entry_time': 1, 'RN_entry.FiO2': 1, 'RN_entry.PEEP': 1, 'RN_entry.Set Vt': 1,
                  '_id': 0})

    rt = RT.find({'RT_entry.FiO2': {'$exists': 1}},
                 {'patientID': 1, 'entry_time': 1, 'RT_entry.FiO2': 1, 'RT_entry.PEEP': 1, 'RT_entry.Set Vt': 1,
                  '_id': 0})

    df = pd.io.json.json_normalize(data)
    df['start_time'] = pd.to_datetime(df['start_time'])
    df.drop_duplicates(subset = 'start_time', keep = 'last', inplace = True)
    df.set_index(['patientID', 'start_time'], inplace = True, verify_integrity = True, drop = False)
    df.sort_index(inplace = True)
    df['vent_settings.PEEP'] = df['vent_settings.PEEP'].astype(np.float64)
    df['breath_number'] = df['breath_number'].astype(np.float64)
    df['analysis.' + ds_types] = df['analysis.' + ds_types].astype(np.float64)

    print('Pre ', df.shape)

    rt_df = pd.io.json.json_normalize(list(rt))
    rn_df = pd.io.json.json_normalize(list(rn))

    rt_df['temp'] = rt_df.RT_entry.apply(unpack)
    rt_df['vent_settings.FiO2'] = rt_df['temp'].apply(lambda x: x[0])
    rt_df['vent_settings.PEEP'] = rt_df['temp'].apply(lambda x: x[1])
    rt_df['vent_settings.set_VT'] = rt_df['temp'].apply(lambda x: x[2])
    rt_df.drop(['RT_entry', 'temp'], inplace = True, axis = 1)

    rn_df['temp'] = rn_df.RN_entry.apply(unpack)
    rn_df['vent_settings.FiO2'] = rn_df['temp'].apply(lambda x: x[0])
    rn_df['vent_settings.PEEP'] = rn_df['temp'].apply(lambda x: x[1])
    rn_df['vent_settings.set_VT'] = rn_df['temp'].apply(lambda x: x[2])
    rn_df.drop(['RN_entry', 'temp'], inplace = True, axis = 1)

    rt_df.set_index(['patientID', 'entry_time'], inplace = True)
    rn_df.set_index(['patientID', 'entry_time'], inplace = True)

    final = rt_df.combine_first(rn_df)
    final.index.set_names(['patientID', 'start_time'], inplace = True)

    df = df.combine_first(final)
    df.set_index(['start_time'], inplace = True)

    print('Post ', df.shape)
    print(df.columns)

    grouped_df = df.groupby('patientID')

    resampled_df = pd.DataFrame()
    for name, group in grouped_df:
        group = group.resample('2H', how = custom_resampler)

        try:
            group['ds_freq'] = group['analysis.' + ds_types] / group['breath_number']
        except ZeroDivisionError:
            print(group['analysis.' + ds_types, 'breath_number'])

        for lags in [12, 18, 24]:
            for items in ['ds', 'FiO2', 'PEEP', 'p_peak', 'set_VT']:
                if items == 'ds':
                    group[items + '_lag_' + str(lags)] = np.nan
                    group[items + '_lag_' + str(lags)] = group['ds_freq'].shift(lags)
                    group[items + '_lag_' + str(lags)] = group[items + '_lag_' + str(lags)].astype(np.float64)
                else:
                    group[items + '_diff_lag_' + str(lags)] = group['vent_settings.' + items] - group[
                        'vent_settings.' + items].shift(lags)

        resampled_df = pd.concat([resampled_df, group])

    resampled_df.dropna(how = 'any', subset = ['vent_settings.FiO2'], inplace = True)
    resampled_df.replace({0: np.nan}, inplace = True)

    for times in ['12', '18', '24']:
        resampled_df['ds_lag_' + times + '_bin'] = resampled_df['ds_lag_' + times].apply(bin_samples)

    resampled_df.to_csv('c:\Research_data\lagged_analysis_2_' + ds_types + '.csv')
