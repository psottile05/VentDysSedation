import pymongo
import pandas as pd
import numpy as np

client = pymongo.MongoClient()
db = client.VentDyssynchrony_db
ventSettings = db.VentSettings_collection
breathData = db.BreathData_collection


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


for ds_types in ['ds', 'pl', 'pvt', 'ie']:
    data = breathData.find({},
                           {'patientID': 1, 'start_time': 1, 'breath_number': 1, 'vent_settings.PEEP': 1,
                            'vent_settings.FiO2': 1, 'vent_settings.compliance': 1, 'vent_settings.set_VT': 1,
                            'vent_settings.p_peak': 1, 'analysis.' + ds_types: 1, '_id': 0})

    df = pd.io.json.json_normalize(data)
    df['start_time'] = pd.to_datetime(df['start_time'])
    df.set_index('start_time', inplace = True)
    df.sort_index(inplace = True)
    df['vent_settings.PEEP'] = df['vent_settings.PEEP'].astype(np.float64)

    print(df.columns)

    grouped_df = df.groupby('patientID')

    resampled_df = pd.DataFrame()
    for name, group in grouped_df:
        group = group.resample('6H', how = custom_resampler)
        group['ds_freq'] = group['analysis.' + ds_types] / group['breath_number']

        for lags in [4, 6, 8]:
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

    for times in ['4', '6', '8']:
        resampled_df['ds_lag_' + times + '_bin'] = resampled_df['ds_lag_' + times].apply(bin_samples)

    resampled_df.to_csv('c:\Research_data\lagged_analysis_' + ds_types + '.csv')
