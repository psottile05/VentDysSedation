__author__ = 'sottilep'

from gevent import monkey
monkey.patch_all()

from pymongo import MongoClient, errors
from CreationModules import FileSearch as FS
import re
import pandas as pd
import numpy as np
import scipy.signal as sig

client = MongoClient()
db = client.VentDB

input_log = db.input_log
breath_col = db.breath_collection
breath_col.drop()

# Update List of RawDataFiles
FS.file_search()

# Query DB for list of files not yet loaded
files = list(input_log.find({'type': 'waveform', 'loaded': 0}).limit(5))

def breath_data(group):
    start_time = group.time.min()
    end_time = group.time.max()
    elapse_time = end_time - start_time
    if group[group.status == 1].time.max() is not np.nan:
        end_insp_time = group[group.status > 0].time.max()
    else:
        end_insp_time = group[group.status == 0].time.min()

    calc_dict = {'dF/dV_insp_max': group[np.abs(group['dF/dV'] == np.inf) & (group.time <= end_insp_time)].time.values,
                 'dP/dV_insp_max': group[np.abs(group['dP/dV'] == np.inf) & (group.time <= end_insp_time)].time.values,
                 'dF/dP_insp_max': group[np.abs(group['dF/dP'] == np.inf) & (group.time <= end_insp_time)].time.values,

                 'dF/dV_exp_max': group[np.abs(group['dF/dV'] == np.inf) & (group.time >= end_insp_time)].time.values,
                 'dP/dV_exp_max': group[np.abs(group['dP/dV'] == np.inf) & (group.time >= end_insp_time)].time.values,
                 'dF/dP_exp_max': group[np.abs(group['dF/dP'] == np.inf) & (group.time >= end_insp_time)].time.values,
                 }

    normalize_list = ['dF/dV_insp_max', 'dP/dV_insp_max', 'dF/dP_insp_max', 'dF/dV_exp_max', 'dP/dV_exp_max',
                      'dF/dP_exp_max']

    for item in normalize_list:
        calc_dict['norm_' + item] = (calc_dict[item] - start_time) / elapse_time
        calc_dict[item] = np.resize(calc_dict[item], (15,))
        calc_dict['norm_' + item] = np.resize(calc_dict['norm_' + item], (15,))
    calc_inner_df = pd.DataFrame.from_dict(calc_dict, orient='index').T.convert_objects()

    breath_dict = {
        'start_time': start_time,
        'end_insp_time': end_insp_time,
        'end_time': end_time,
        'insp_time': end_insp_time - start_time,
        'exp_time': end_time - end_insp_time,
        'elapse_time': end_time - start_time,

        'peak_paw': group.paw.max(),
        'mean_insp_paw': group[group.time <= end_insp_time].paw.mean(),
        'end_insp_paw': group[group.time == end_insp_time].paw.max(),
        'mean_exp_paw': group[group.time >= end_insp_time].paw.mean(),
        'min_paw': group.paw.min(),

        'peak_flow': group.flow.max(),
        'mean_insp_flow': group[group.time <= end_insp_time].flow.mean(),
        'end_insp_flow': group[group.time == end_insp_time].flow.min(),
        'mean_exp_flow': group[group.time >= end_insp_time].flow.mean(),
        'min_flow': group.flow.min(),

        'peak_vol': group.vol.max(),
        'end_insp_vol': group[group.time == end_insp_time].vol.min(),
        'min_vol': group.vol.min(),

        'n_dF/dV_insp_max': int(calc_inner_df[calc_inner_df['dF/dV_insp_max'] > 0]['dF/dV_insp_max'].count()),
        'n_dP/dV_insp_max': int(calc_inner_df[calc_inner_df['dP/dV_insp_max'] > 0]['dP/dV_insp_max'].count()),
        'n_dF/dP_insp_max': int(calc_inner_df[calc_inner_df['dF/dP_insp_max'] > 0]['dF/dP_insp_max'].count()),

        'n_dF/dV_exp_max': int(calc_inner_df[calc_inner_df['dF/dV_exp_max'] > 0]['dF/dV_exp_max'].count()),
        'n_dP/dV_exp_max': int(calc_inner_df[calc_inner_df['dP/dV_exp_max'] > 0]['dP/dV_exp_max'].count()),
        'n_dF/dP_exp_max': int(calc_inner_df[calc_inner_df['dF/dP_exp_max'] > 0]['dF/dP_exp_max'].count())
    }

    raw_dict = {
        'time': group.time.values.tolist(),
        'breath': group.breath.values.tolist(),
        'status': group.status.values.tolist(),
        'paw': group.paw.values.tolist(),
        'flow': group.flow.values.tolist(),
        'vol': group.vol.values.tolist(),
        'sm_paw': group.sm_paw.values.tolist(),
        'sm_flow': group.sm_flow.values.tolist(),
        'sm_vol': group.sm_vol.values.tolist(),
        'dF/dV': group['dF/dV'].values.tolist(),
        'dP/dV': group['dP/dV'].values.tolist(),
        'dF/dP': group['dF/dP'].values.tolist()
    }

    mongo_record = {
        '_id': group.file.head(1).values.tolist()[0] + '/' + str(group.breath.min()) + '/' + str(group.date_time.min())
               + '/' +str(start_time),
        'patient_ID': int(group.patient_ID.head(1)),
        'file': group.file.head(1).values.tolist()[0],
        'breath_num': group.breath.min(),
        'date_time': group.date_time.dt.to_pydatetime().min(),
        'breath_raw': raw_dict,
        'breath_character': breath_dict,
        'breath_derivative': calc_inner_df.to_dict(orient='list')
    }

    try:
        breath_col.insert_one(mongo_record)
    except errors.DuplicateKeyError:
        if group.breath.min() == 0:
            pass
        else:
            print('Dup Key Error: ', mongo_record['_id'])
            pass

    return


for file in files:
    df = pd.read_csv(file['_id'], sep = '\t', header = 1, na_values = '--',
                     usecols = ['Date', 'HH:MM:SS', 'Time(ms)', 'Breath', 'Status', 'Paw (cmH2O)', 'Flow (l/min)',
                                'Volume (ml)'])
    df['date_time'] = pd.to_datetime(df['Date'] + ' ' + df['HH:MM:SS'], coerce = True, dayfirst = True,
                                     infer_datetime_format = True)
    df.drop(['Date', 'HH:MM:SS'], axis = 1, inplace = True)
    df.rename(columns = {'Time(ms)': 'time', 'Breath': 'breath', 'Status': 'status', 'Paw (cmH2O)': 'paw',
                         'Flow (l/min)': 'flow', 'Volume (ml)': 'vol'}, inplace = True)

    df['patient_ID'] = int(re.search('(?<=P)[0-9]*', file['_id']).group())
    df['file'] = str(re.search('(?<=\d\d/).*', file['_id']).group())
    df['sm_vol'] = sig.savgol_filter(df.vol.values, window_length = 7, polyorder = 2)
    df['sm_paw'] = sig.savgol_filter(df.paw.values, window_length = 7, polyorder = 2)
    df['sm_flow'] = sig.savgol_filter(df.flow.values, window_length = 7, polyorder = 2)

    df['vol_dt'] = df.vol.diff()
    df['flow_dt'] = df.flow.diff()
    df['paw_dt'] = df.paw.diff()
    df['dF/dV'] = df.flow_dt / df.vol_dt
    df['dP/dV'] = df.paw_dt / df.vol_dt
    df['dF/dP'] = df.flow_dt / df.paw_dt

    breath_df = df.groupby('breath')
    breath_df.apply(breath_data)
