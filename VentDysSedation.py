__author__ = 'sottilep'

from pymongo import MongoClient, errors
from CreationModules import FileSearch as FS
import pandas as pd
import numpy as np
import scipy.signal as sig

client = MongoClient()
db = client.VentDB
input_log = db.input_log

#Update List of RawDataFiles
FS.file_search()

#Query DB for list of files not yet loaded
files = list(input_log.find({'type':'waveform', 'loaded':0}).limit(1))

def breath_data(group):
    start_time = group.time.min()
    end_time = group.time.max()

    if group[group.status==1].time.max() is not np.nan:
        end_insp_time = group[group.status > 0].time.max()
    else:
        end_insp_time = group[group.status == 0].time.min()

    breath_dict = {'breath_num': group.breath.min(),
                 'date_time': group.date_time.head(1),
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
                 'min_paw':group.paw.min(),

                 'peak_flow': group.flow.max(),
                 'mean_insp_flow': group[group.time <= end_insp_time].flow.mean(),
                 'end_insp_flow': group[group.time == end_insp_time].flow.min(),
                 'mean_exp_flow': group[group.time >= end_insp_time].flow.mean(),
                 'min_flow': group.flow.min(),

                 'peak_vol': group.vol.max(),
                 'end_insp_vol':  group[group.time == end_insp_time].vol.min(),
                 'min_vol':  group.vol.min(),
    }

    calc_dict={  'dF/dV_insp_max': group[np.abs(group['dF/dV'] == np.inf) & (group.time <= end_insp_time)].time.values,
                 'dP/dV_insp_max': group[np.abs(group['dP/dV'] == np.inf) & (group.time <= end_insp_time)].time.values,
                 'dF/dP_insp_max': group[np.abs(group['dF/dP'] == np.inf) & (group.time <= end_insp_time)].time.values,

                 'dF/dV_exp_max': group[np.abs(group['dF/dV'] == np.inf) & (group.time >= end_insp_time)].time.values,
                 'dP/dV_exp_max': group[np.abs(group['dP/dV'] == np.inf) & (group.time >= end_insp_time)].time.values,
                 'dF/dP_exp_max': group[np.abs(group['dF/dP'] == np.inf) & (group.time >= end_insp_time)].time.values,
    }

    normalize_list = ['dF/dV_insp_max','dP/dV_insp_max','dF/dP_insp_max', 'dF/dV_exp_max', 'dP/dV_exp_max', 'dF/dP_exp_max']
    for item in normalize_list:
        calc_dict['norm_'+ item] = (calc_dict[item] - breath_dict['start_time'])/breath_dict['elapse_time']

    print(pd.DataFrame.from_dict(calc_dict), orient='index')
    return pd.DataFrame.from_dict(breath_dict, orient='index').T


for file in files:
    df = pd.read_csv(file['_id'], sep = '\t', header=1, na_values='--',
                     usecols=['Date', 'HH:MM:SS', 'Time(ms)', 'Breath', 'Status', 'Paw (cmH2O)', 'Flow (l/min)', 'Volume (ml)'])
    df['date_time'] = pd.to_datetime(df['Date'] +' '+ df['HH:MM:SS'], coerce=True, dayfirst=True, infer_datetime_format=True)
    df.drop(['Date', 'HH:MM:SS'], axis=1, inplace=True)
    df.rename(columns={'Time(ms)': 'time', 'Breath': 'breath', 'Status': 'status', 'Paw (cmH2O)': 'paw',
                       'Flow (l/min)': 'flow', 'Volume (ml)': 'vol'}, inplace=True)

    df['sm_vol'] = sig.savgol_filter(df.vol.values, window_length=7, polyorder=2)
    df['sm_paw'] = sig.savgol_filter(df.paw.values, window_length=7, polyorder=2)
    df['sm_flow'] = sig.savgol_filter(df.flow.values, window_length=7, polyorder=2)

    df['vol_dt'] = df.vol.diff()
    df['flow_dt'] = df.flow.diff()
    df['paw_dt'] = df.paw.diff()
    df['dF/dV'] = df.flow_dt/df.vol_dt
    df['dP/dV'] = df.paw_dt/df.vol_dt
    df['dF/dP'] = df.flow_dt/df.paw_dt

    breath_df = df.groupby('breath')

    agg_df = pd.DataFrame()
    agg_df = pd.concat([agg_df, breath_df.apply(breath_data)])

#print(agg_df.shape)
#print(agg_df.describe())

