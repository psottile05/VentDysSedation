__author__ = 'sottilep'

from pymongo import MongoClient
import pandas as pd

def data_collect(patient):
    client = MongoClient()
    # print(client.database_names())

    db = client.VentDyssynchrony_db
    #print(db.collection_names())

    breath_data = db.BreathData_collection
    rn_data = db.RNData_collection

    results = breath_data.find({'patientID': patient},
                               {'_id': 0, 'patientID': 1, 'start_time': 1, 'analysis': 1})
    breath_df = pd.io.json.json_normalize(results)
    breath_df.set_index(['start_time'], inplace = True)
    breath_df['patientID'] = breath_df['patientID'].str.lstrip('P').astype(int)
    breath_df = breath_df.resample('1s', limit = 20)

    results = rn_data.find({'patientID': patient, 'RN_entry.RASS': {'$exists': 1}},
                           {'_id': 0, 'patientID': 1, 'entry_time': 1, 'RN_entry': 1})
    rn_df = pd.DataFrame.from_dict(list(results))
    rn_df.set_index(['entry_time'], inplace = True)
    rn_df['patientID'] = rn_df['patientID'].str.lstrip('P').astype(int)
    rn_df['RASS'] = rn_df['RN_entry'].apply(lambda x: x[0]['RASS'])

    return breath_df, rn_df


def collection_freq(breath_df, win):
    for ds_type in ['ds', 'pl', 'ie']:
        breath_df[ds_type + '_rolling'] = pd.rolling_sum(breath_df['analysis.' + ds_type], window = 60 * win,
                                                         center = True,
                                             min_periods = 1)
        breath_df[ds_type + '_tot_rolling'] = pd.rolling_count(breath_df['analysis.' + ds_type], window = 60 * win,
                                                              center = True)
        breath_df[ds_type + '_ds_freq'] = breath_df[ds_type + '_rolling'] / breath_df[ds_type + '_tot_rolling']
        print(breath_df.columns)
    return breath_df


def rolling_rass_combi(breath_df, rn_df):
    combi_df = rn_df.join(breath_df, how = 'left', lsuffix = '_l')
    combi_df.drop(
        ['analysis.ds', 'analysis.fl', 'analysis.ie', 'analysis.pl', 'analysis.pvt', 'RN_entry', 'patientID_l'],
        axis = 1, inplace = True)
    combi_df.dropna(axis = 0, how = 'any', subset = ['ds_freq'], inplace = True)

    return combi_df


def get_data(patient_list, win_range):
    total_df = pd.DataFrame()
    for items in patient_list:
        breath_df, rn_df = data_collect(items)
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

