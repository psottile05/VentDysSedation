__author__ = 'sottilep'

from pymongo import MongoClient
import numpy as np
import pandas as pd
import re
import scipy.signal as sig

client = MongoClient()
db = client.VentDB

input_log = db.input_log
breath_col = db.breath_collection

def get_waveform_data(file):
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

    return df


def get_breath_data(file):
    if isinstance(file['match_file'], type('String')):
        df = pd.read_csv(file['match_file'], sep = '\t', header = 1, na_values = '--',
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
                             'TE (s)': 't_exp', 'Cstat (ml/cmH2O)': 'complaince', 'TI (s)': 't_insp'}, inplace = True)
        df['date_time'] = pd.to_datetime(df['Date'] + ' ' + df['HH:MM:SS'], coerce = True, dayfirst = True,
                                         infer_datetime_format = True)
        df['patient_ID'] = int(file['patient_id'])
        df.drop(['Date', 'HH:MM:SS'], axis = 1, inplace = True)
    else:
        df = pd.DataFrame()

    return df


def breath_data_entry(df):
    pass

def waveform_data_entry(group):
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
    calc_inner_df = pd.DataFrame.from_dict(calc_dict, orient = 'index').T.convert_objects()

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
               + '/' + str(start_time),
        'patient_ID': int(group.patient_ID.head(1)),
        'file': group.file.head(1).values.tolist()[0],
        'breath_num': group.breath.min(),
        'date_time': group.date_time.dt.to_pydatetime().min(),
        'loc':[group.date_time.dt.to_pydatetime().min().timestamp(), int(group.patient_ID.head(1))],
        'breath_raw': raw_dict,
        'breath_character': breath_dict,
        'breath_derivative': calc_inner_df.to_dict(orient = 'list')
    }

    return mongo_record
