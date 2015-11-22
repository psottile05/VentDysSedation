__author__ = 'sottilep'

import re
import numpy as np
import pandas as pd
import json
import scipy.signal as sig
from pymongo import MongoClient

client = MongoClient()
db = client.VentDB

input_log = db.input_log
breath_col = db.breath_collection


def date_check(df):
    if df[(df['date_time'].dt.year < 2014) | (df['date_time'].dt.year > 2016)]['date_time'].any():
        print('Year out of range', df['date_time'].dt.year.min())


def dtype_check(df, types):
    for col in df.columns:
        try:
            assert df[col].dtype == types[col]
        except AssertionError:
            if df[col].dtype != 'object':
                print('more specific dtype')
                pass
            else:
                print('Assertion Error at ' + col + ' for P' + str(df['patient_ID'].head(1).values.tolist()[0]) + '/'
                      + str(df['file'].head(1).values.tolist()[0]))
                print('Dtype is ' + str(df[col].dtype) + ' but should be ' + types[col])
                print(df[col])


def align_breath(group, breath_df):
    breath_setting = breath_df[breath_df.index == group.date_time.min()]
    breath_setting_temp = breath_setting.to_dict(orient = 'records')

    if len(breath_setting_temp) > 0:
        breath_setting = breath_setting_temp[0]
    else:
        breath_setting = {}

    return breath_setting


def get_breath_data(file):
    if isinstance(file['match_file'], type('String')):
        df = pd.read_csv(file['match_file'], sep = '\t', header = 1, na_values = '--', engine = 'c',
                         usecols = ['Date', 'HH:MM:SS', 'Vt (ml)', 'PeakFlow (l/min)',
                                    'Ptrigg (cmH2O)', 'Peep (cmH2O)', 'Psupp (cmH2O)',
                                    'Mode', 'Oxygen (%)', 'Trigger', 'I:E',
                                    'Ramp (ms)', 'VTI (ml)', 'VTE (ml)',
                                    'ExpMinVol (l/min)', 'Insp flow (l/min)', 'Vt leak (ml)',
                                    'Exp flow (l/min)', 'P peak (cmH2O)', 'P mean (cmH2O)',
                                    'P plateau (cmH2O)', 'AutoPEEP (cmH2O)',
                                    'P min (cmH2O)', 'Pinsp (cmH2O)', 'f total (b/min)',
                                    'TE (s)', 'Cstat (ml/cmH2O)', 'TI (s)'])
        df.rename(columns = {'Vt (ml)': 'set_VT', 'PeakFlow (l/min)': 'peak_flow',
                             'Ptrigg (cmH2O)': 'ptrigg', 'Peep (cmH2O)': 'peep', 'Psupp (cmH2O)': 'psupp',
                             'Mode': 'vent_mode', 'Oxygen (%)': 'fio2', 'Trigger': 'tigger', 'I:E': 'i:e',
                             'Ramp (ms)': 'ramp', 'VTI (ml)': 'vti', 'VTE (ml)': 'vte',
                             'ExpMinVol (l/min)': 'exp_minute_vol', 'Insp flow (l/min)': 'insp_flow',
                             'Vt leak (ml)': 'leak', 'Exp flow (l/min)': 'exp_flow', 'P peak (cmH2O)': 'peak_paw',
                             'P mean (cmH2O)': 'mean_paw',
                             'P plateau (cmH2O)': 'plat_paw', 'AutoPEEP (cmH2O)': 'auto_peep',
                             'P min (cmH2O)': 'min_paw', 'Pinsp (cmH2O)': 'insp_paw', 'f total (b/min)': 'rr',
                             'TE (s)': 't_exp', 'Cstat (ml/cmH2O)': 'compliance', 'TI (s)': 't_insp'}, inplace = True)
        df['date_time'] = pd.to_datetime(df['Date'] + ' ' + df['HH:MM:SS'], errors = 'raise',
                                         format = '%d.%m.%y %H:%M:%S')
        df['patient_ID'] = int(file['patient_id'])
        df['file'] = file['match_file']
        df.drop(['Date', 'HH:MM:SS', 'patient_ID'], axis = 1, inplace = True)

        types = {'set_VT': 'float64', 'peak_flow': 'float64', 'ptrigg': 'float64', 'peep': 'float64',
                 'psupp': 'float64', 'file': 'object',
                 'vent_mode': 'object', 'fio2': 'float64', 'tigger': 'float64', 'i:e': 'object', 'ramp': 'float64',
                 'vti': 'float64', 'vte': 'float64', 'exp_minute_vol': 'float64', 'insp_flow': 'float64',
                 'leak': 'float64',
                 'exp_flow': 'float64', 'peak_paw': 'float64', 'mean_paw': 'float64', 'plat_paw': 'float64',
                 'auto_peep': 'float64', 'min_paw': 'float64', 'insp_paw': 'float64', 'rr': 'float64',
                 't_exp': 'float64',
                 'compliance': 'float64', 't_insp': 'float64', 'date_time': 'datetime64[ns]', 'patient_ID': 'int64'}

        date_check(df)
        dtype_check(df, types)

        df.set_index(['date_time'], inplace = True)
        df = df.resample('1s', fill_method = 'pad', limit = 30)

    else:
        print('missing breath file')
        df = pd.DataFrame()

    return df


def get_waveform_data(file):
    df = pd.read_csv(file['_id'], sep = '\t', header = 1, na_values = '--', engine = 'c',
                     usecols = ['Date', 'HH:MM:SS', 'Time(ms)', 'Breath', 'Status', 'Paw (cmH2O)', 'Flow (l/min)',
                                'Volume (ml)'])
    df['date_time'] = pd.to_datetime(df['Date'] + ' ' + df['HH:MM:SS'], errors = 'raise',
                                     format = '%d.%m.%y %H:%M:%S')
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

    types = {'time': 'int64', 'breath': 'int64', 'status': 'int64', 'paw': 'float64', 'flow': 'float64',
             'vol': 'float64',
             'date_time': 'datetime64[ns]', 'patient_ID': 'int64', 'file': 'object', 'sm_vol': 'float64',
             'sm_paw': 'float64',
             'sm_flow': 'float64', 'vol_dt': 'float64', 'flow_dt': 'float64', 'paw_dt': 'float64', 'dF/dV': 'float64',
             'dP/dV': 'float64', 'dF/dP': 'float64'}

    date_check(df)
    dtype_check(df, types)

    return df


def waveform_data_entry(group, breath_df):
    start_time = group.time.min()
    end_time = group.time.max()
    elapse_time = end_time - start_time

    if group.status.any() > 0:
        end_insp_time = group[group.status > 0].time.max()
    else:
        end_insp_time = group[group.status == 0].time.min()

    insp_df = group[group.time < end_insp_time]
    exp_df = group[group.time > end_insp_time]
    end_insp_df = group[group.time == end_insp_time]

    calc_dict = {'dF/dV_insp_max': insp_df[np.abs(insp_df['dF/dV'] == np.inf)].time.values,
                 'dP/dV_insp_max': insp_df[np.abs(insp_df['dP/dV'] == np.inf)].time.values,
                 'dF/dP_insp_max': insp_df[np.abs(insp_df['dF/dP'] == np.inf)].time.values,

                 'dF/dV_exp_max': exp_df[np.abs(exp_df['dF/dV'] == np.inf)].time.values,
                 'dP/dV_exp_max': exp_df[np.abs(exp_df['dP/dV'] == np.inf)].time.values,
                 'dF/dP_exp_max': exp_df[np.abs(exp_df['dF/dP'] == np.inf)].time.values,
                 }

    normalize_list = ['dF/dV_insp_max', 'dP/dV_insp_max', 'dF/dP_insp_max', 'dF/dV_exp_max', 'dP/dV_exp_max',
                      'dF/dP_exp_max']

    for item in normalize_list:
        calc_dict['norm_' + item] = (calc_dict[item] - start_time) / elapse_time
        calc_dict[item] = np.resize(calc_dict[item], (15,))
        calc_dict['norm_' + item] = np.resize(calc_dict['norm_' + item], (15,))
    calc_inner_df = pd.DataFrame.from_dict(calc_dict, orient = 'index').T.convert_objects()

    types = {'norm_dP/dV_exp_max': 'float64', 'dP/dV_exp_max': 'float64', 'norm_dP/dV_insp_max': 'float64',
             'norm_dF/dV_exp_max': 'float64', 'dF/dV_insp_max': 'float64', 'dF/dV_exp_max': 'float64',
             'norm_dF/dV_insp_max': 'float64', 'norm_dF/dP_exp_max': 'float64', 'dF/dP_exp_max': 'float64',
             'dP/dV_insp_max': 'float64', 'dF/dP_insp_max': 'float64', 'norm_dF/dP_insp_max': 'float64'}
    dtype_check(calc_inner_df, types)

    breath_dict = {
        'start_time': start_time,
        'end_insp_time': end_insp_time,
        'end_time': end_time,
        'insp_time': end_insp_time - start_time,
        'exp_time': end_time - end_insp_time,
        'elapse_time': end_time - start_time,

        'peak_paw': group.paw.max(),
        'mean_insp_paw': insp_df.paw.mean(),
        'end_insp_paw': end_insp_df.paw.max(),
        'mean_exp_paw': exp_df.paw.mean(),
        'min_paw': group.paw.min(),

        'peak_flow': group.flow.max(),
        'mean_insp_flow': insp_df.flow.mean(),
        'end_insp_flow': end_insp_df.flow.min(),
        'mean_exp_flow': exp_df.flow.mean(),
        'min_flow': group.flow.min(),

        'peak_vol': group.vol.max(),
        'end_insp_vol': end_insp_df.vol.min(),
        'min_vol': group.vol.min(),

        'n_dF/dV_insp_max': int(calc_inner_df[calc_inner_df['dF/dV_insp_max'] > 0]['dF/dV_insp_max'].count()),
        'n_dP/dV_insp_max': int(calc_inner_df[calc_inner_df['dP/dV_insp_max'] > 0]['dP/dV_insp_max'].count()),
        'n_dF/dP_insp_max': int(calc_inner_df[calc_inner_df['dF/dP_insp_max'] > 0]['dF/dP_insp_max'].count()),

        'n_dF/dV_exp_max': int(calc_inner_df[calc_inner_df['dF/dV_exp_max'] > 0]['dF/dV_exp_max'].count()),
        'n_dP/dV_exp_max': int(calc_inner_df[calc_inner_df['dP/dV_exp_max'] > 0]['dP/dV_exp_max'].count()),
        'n_dF/dP_exp_max': int(calc_inner_df[calc_inner_df['dF/dP_exp_max'] > 0]['dF/dP_exp_max'].count())
    }

    raw_dict = {
        'time': group.time.values,
        'breath': group.breath.values,
        'status': group.status.values,
        'paw': group.paw.values,
        'flow': group.flow.values,
        'vol': group.vol.values,
        'sm_paw': group.sm_paw.values,
        'sm_flow': group.sm_flow.values,
        'sm_vol': group.sm_vol.values,
        'dF/dV': group['dF/dV'].values,
        'dP/dV': group['dP/dV'].values,
        'dF/dP': group['dF/dP'].values
    }

    breath_setting = align_breath(group, breath_df)

    mongo_record = {
        '_id': group.file.head(1).values.tolist()[0] + '/' + str(group.breath.min()) + '/' + str(group.date_time.min())
               + '/' + str(start_time),
        'patient_ID': int(group.patient_ID.head(1)),
        'file': group.file.head(1).values[0],
        'breath_num': group.breath.min(),
        'date_time': group.date_time.min().timestamp(),
        'loc': [group.date_time.min().timestamp(), int(group.patient_ID.head(1))],
        'breath_settings': breath_setting,
        'breath_raw': raw_dict,
        'breath_character': breath_dict,
        'breath_derivative': calc_inner_df.to_dict(orient = 'list')
    }

    return mongo_record


def get_waveform_and_breath(file):
    breath_df = get_breath_data(file)
    wave_df = get_waveform_data(file)

    breath_col.insert_many(
        json.loads(
            wave_df.groupby('breath', sort = False).apply(waveform_data_entry,
                                                          breath_df = breath_df).to_json(orient = 'records')),
        ordered = False)
    input_log.update_one({'_id': file['_id']}, {'$set': {'loaded': 1}})
    input_log.update_one({'_id': file['match_file']}, {'$set': {'loaded': 1, 'crossed': 1}})
