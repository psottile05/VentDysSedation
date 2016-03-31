import os

import numpy as np
import pandas as pd
import scipy.signal as sig
from pymongo import MongoClient, bulk, errors

import AnalysisModules.WaveformAnalysis as WA

__author__ = 'sottilep'

client = MongoClient()
db = client.VentDB

input_log = db.input_log
breath_col = db.breath_collection


def date_check(df, file):
    try:
        if df[(df['date_time'].dt.year < 2014) | (df['date_time'].dt.year > 2016)]['date_time'].any():
            print('Year out of range', df['date_time'].dt.year.min())
            input_log.update_one({'_id': file['_id']},
                                 {'$addToSet': {'errors': 'date_range_error', 'date_range_error': file['_id']}})
    except AttributeError:
        print('unable to assess date_time')


def dtype_check(df, types, file):
    for col in df.columns:
        try:
            assert df[col].dtype == types[col]
        except AssertionError:
            if df[col].dtype == 'object':
                print('Assertion Error at ' + col + ' for P' + str(df['patient_ID'].head(1).values.tolist()[0]) + '/'
                      + str(df['file'].head(1).values.tolist()[0]))
                print('Dtype is ' + str(df[col].dtype) + ' but should be ' + types[col])

                try:
                    df[col] = df[col].astype(types[col])
                    print('Successful conversion')
                except:
                    print('failed conversion')
                    print(df[col])
                    input_log.update_one({'_id': file['_id']},
                                         {'$addToSet': {'errors': 'dtype_error', 'dtype_error': df['file']}})
            else:
                df[col] = df[col].astype(types[col])


def align_breath(group, breath_df, file):
    breath_setting = breath_df[breath_df.index == group.date_time.min()]
    breath_setting_temp = breath_setting.to_dict(orient = 'records')

    if len(breath_setting_temp) > 0:
        breath_setting = breath_setting_temp[0]

    else:
        print('align error', file)
        breath_setting = {'set_VT': np.nan, 'peak_flow': np.nan, 'ptrigg': np.nan, 'peep': np.nan, 'psupp': np.nan,
                          'fio2': np.nan, 'tigger': np.nan, 'ramp': np.nan, 'vti': np.nan, 'vte': np.nan,
                          'exp_minute_vol': np.nan, 'insp_flow': np.nan, 'leak': np.nan, 'exp_flow': np.nan,
                          'peak_paw': np.nan, 'mean_paw': np.nan, 'plat_paw': np.nan, 'auto_peep': np.nan,
                          'min_paw': np.nan, 'insp_paw': np.nan, 'rr': np.nan, 't_exp': np.nan, 'compliance': np.nan,
                          't_insp': np.nan, 'high_paw_alarm': np.nan}
        input_log.update_one({'_id': file['_id']},
                             {'$addToSet': {'warnings': 'align_warning',
                                            'align_warning': int(group['breath'].head(1))}})

    return breath_setting


def get_breath_data(file):
    try:
        match_file = input_log.find_one({'_id': file['match_file']}, {'file_name': 1})
        file_path = None
        if os.name == 'nt':
            for names in match_file['file_name']['nt']:
                if os.path.exists(names):
                    file_path = names
            if file_path is None:
                input_log.update_one({'_id': file['_id']},
                                     {'$addToSet': {'errors': 'miss_match_file_path',
                                                    'miss_match_file_path': match_file['file_name']}})
        elif os.name == 'posix':
            for names in match_file['file_name']['posix']:
                if os.path.exists(names):
                    file_path = names
            if file_path is None:
                input_log.update_one({'_id': file['_id']},
                                     {'$addToSet': {'errors': 'miss_match_file_path',
                                                    'miss_match_file_path': match_file['file_name']}})
    except KeyError:
        file_path = None

    if isinstance(file_path, type('String')):
        try:
            df = pd.read_csv(file_path, sep = '\t', header = 1, na_values = '--', engine = 'c',
                             usecols = ['Date', 'HH:MM:SS', 'Vt (ml)', 'PeakFlow (l/min)',
                                        'Ptrigg (cmH2O)', 'Peep (cmH2O)', 'Psupp (cmH2O)',
                                    'Mode', 'Oxygen (%)', 'Trigger', 'I:E',
                                    'Ramp (ms)', 'VTI (ml)', 'VTE (ml)',
                                    'ExpMinVol (l/min)', 'Insp flow (l/min)', 'Vt leak (ml)',
                                    'Exp flow (l/min)', 'P peak (cmH2O)', 'P mean (cmH2O)',
                                    'P plateau (cmH2O)', 'AutoPEEP (cmH2O)',
                                    'P min (cmH2O)', 'Pinsp (cmH2O)', 'f total (b/min)',
                                        'TE (s)', 'Cstat (ml/cmH2O)', 'TI (s)', '!High Pressure'],
                             low_memory = False
                             )
            df.rename(columns = {'Vt (ml)': 'set_VT', 'PeakFlow (l/min)': 'peak_flow',
                                 'Ptrigg (cmH2O)': 'ptrigg', 'Peep (cmH2O)': 'peep', 'Psupp (cmH2O)': 'psupp',
                                 'Mode': 'vent_mode', 'Oxygen (%)': 'fio2', 'Trigger': 'tigger', 'I:E': 'i:e',
                                 'Ramp (ms)': 'ramp', 'VTI (ml)': 'vti', 'VTE (ml)': 'vte',
                                 'ExpMinVol (l/min)': 'exp_minute_vol', 'Insp flow (l/min)': 'insp_flow',
                                 'Vt leak (ml)': 'leak', 'Exp flow (l/min)': 'exp_flow', 'P peak (cmH2O)': 'peak_paw',
                                 'P mean (cmH2O)': 'mean_paw', '!High Pressure': 'high_paw_alarm',
                                 'P plateau (cmH2O)': 'plat_paw', 'AutoPEEP (cmH2O)': 'auto_peep',
                                 'P min (cmH2O)': 'min_paw', 'Pinsp (cmH2O)': 'insp_paw', 'f total (b/min)': 'rr',
                                 'TE (s)': 't_exp', 'Cstat (ml/cmH2O)': 'compliance', 'TI (s)': 't_insp'},
                      inplace = True)
        except:
            df = pd.DataFrame({'Date': [np.nan], 'HH:MM:SS': [np.nan], 'set_VT': [np.nan],
                               'peak_flow': [np.nan], 'ptrigg': [np.nan],
                               'peep': [np.nan], 'psupp': [np.nan],
                               'vent_mode': [np.nan], 'fio2': [np.nan], 'tigger': [np.nan], 'i:e': [np.nan],
                               'ramp': [np.nan], 'vti': [np.nan], 'vte': [np.nan],
                               'exp_minute_vol': [np.nan], 'insp_flow': [np.nan],
                               'leak': [np.nan], 'exp_flow': [np.nan], 'peak_paw': [np.nan],
                               'mean_paw': [np.nan], 'high_paw_alarm': [np.nan],
                               'plat_paw': [np.nan], 'auto_peep': [np.nan],
                               'min_paw': [np.nan], 'insp_paw': [np.nan], 'rr': [np.nan],
                               't_exp': [np.nan], 'compliance': [np.nan], 't_insp': [np.nan]})


        try:
            df['date_time'] = pd.to_datetime(df['Date'] + ' ' + df['HH:MM:SS'], errors = 'raise',
                                             format = '%d.%m.%y %H:%M:%S')
        except ValueError:
            try:
                df['date_time'] = pd.to_datetime(df['Date'] + ' ' + df['HH:MM:SS'], errors = 'coerce',
                                                 infer_datetime_format = True)
            except ValueError:
                input_log.update_one({'_id': file['_id']},
                                     {'$addToSet': {'errors': 'time_parse_error',
                                                    'time_parse_error': file_path}})
                print('datetime failed')
                df['date_time'] = np.nan

        df['patient_ID'] = int(file['patient_id'])
        df['file'] = file['match_file']
        df.drop(['Date', 'HH:MM:SS'], axis = 1, inplace = True)
        df.dropna(subset = ['date_time'], how = 'any', axis = 0, inplace = True)

        types = {'set_VT': 'float64', 'peak_flow': 'float64', 'ptrigg': 'float64', 'peep': 'float64',
                 'psupp': 'float64', 'file': 'object',
                 'vent_mode': 'object', 'fio2': 'float64', 'tigger': 'float64', 'i:e': 'object', 'ramp': 'float64',
                 'vti': 'float64', 'vte': 'float64', 'exp_minute_vol': 'float64', 'insp_flow': 'float64',
                 'leak': 'float64',
                 'exp_flow': 'float64', 'peak_paw': 'float64', 'mean_paw': 'float64', 'plat_paw': 'float64',
                 'auto_peep': 'float64', 'min_paw': 'float64', 'insp_paw': 'float64', 'rr': 'float64',
                 't_exp': 'float64', 'high_paw_alarm': 'float64',
                 'compliance': 'float64', 't_insp': 'float64', 'date_time': 'datetime64[ns]', 'patient_ID': 'int64'}

        date_check(df, file)
        dtype_check(df, types, file)

        df.set_index(['date_time'], inplace = True)
        df.vent_mode = df.vent_mode.astype('category')
        df.file = df.file.astype('category')
        df = df.resample('1s').pad(limit = 30)

    else:
        print('missing breath file')
        df = pd.DataFrame()
        input_log.update_one({'_id': file['_id']},
                             {'$addToSet': {'warnings': 'missing_breath_file_warning',
                                            'missing_breath_file_warning': 'no breath file'}})

    return df


def get_waveform_data(file):
    file_path = None
    if os.name == 'nt':
        for names in file['file_name']['nt']:
            if os.path.exists(names):
                file_path = names
        if file_path is None:
            input_log.update_one({'_id': file['_id']},
                                 {'$addToSet': {'errors': 'missing_file_path',
                                                'missing_file_path': file['file_name']}})
    elif os.name == 'posix':
        for names in file['file_name']['posix']:
            if os.path.exists(names):
                file_path = names
        if file_path is None:
            input_log.update_one({'_id': file['_id']},
                                 {'$addToSet': {'errors': 'missing_file_path', 'missing_file_path': file['file_name']}})

    df = pd.read_csv(file_path, sep = '\t', header = 1, na_values = '--', engine = 'c',
                     usecols = ['Date', 'HH:MM:SS', 'Time(ms)', 'Breath', 'Status', 'Paw (cmH2O)', 'Flow (l/min)',
                                'Volume (ml)'])
    try:
        df['date_time'] = pd.to_datetime(df['Date'] + ' ' + df['HH:MM:SS'], errors = 'raise',
                                         format = '%d.%m.%y %H:%M:%S')
    except ValueError:
        try:
            df['date_time'] = pd.to_datetime(df['Date'] + ' ' + df['HH:MM:SS'], errors = 'raise',
                                             infer_datetime_format = True)
        except ValueError:
            input_log.update_one({'_id': file['_id']},
                                 {'$addToSet': {'errors': 'time_parse_error',
                                                'time_parse_error': file_path}})
            df['date_time'] = np.nan

    df.drop(['Date', 'HH:MM:SS'], axis = 1, inplace = True)
    df.rename(columns = {'Time(ms)': 'time', 'Breath': 'breath', 'Status': 'status', 'Paw (cmH2O)': 'paw',
                         'Flow (l/min)': 'flow', 'Volume (ml)': 'vol'}, inplace = True)
    df['patient_ID'] = int(file['patient_id'])
    df['file'] = file_path
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

    date_check(df, file)
    dtype_check(df, types, file)

    return df


def waveform_data_entry(group, breath_df, file):
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
    dtype_check(calc_inner_df, types, file)

    breath_dict = {
        'start_time': int(start_time),
        'end_insp_time': int(end_insp_time),
        'end_time': int(end_time),
        'insp_time': int(end_insp_time - start_time),
        'exp_time': int(end_time - end_insp_time),
        'elapse_time': int(end_time - start_time),

        'peak_paw': float(group.paw.max()),
        'mean_insp_paw': float(insp_df.paw.mean()),
        'end_insp_paw': float(end_insp_df.paw.max()),
        'mean_exp_paw': float(exp_df.paw.mean()),
        'min_paw': float(group.paw.min()),

        'peak_flow': float(group.flow.max()),
        'mean_insp_flow': float(insp_df.flow.mean()),
        'end_insp_flow': float(end_insp_df.flow.min()),
        'mean_exp_flow': float(exp_df.flow.mean()),
        'min_flow': float(group.flow.min()),

        'peak_vol': float(group.vol.max()),
        'end_insp_vol': float(end_insp_df.vol.min()),
        'min_vol': float(group.vol.min()),

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
        'dF/dT': group['flow_dt'].values.tolist(),
        'dP/dT': group['paw_dt'].values.tolist(),
        'dV/dT': group['vol_dt'].values.tolist(),
        'dF/dV': group['dF/dV'].values.tolist(),
        'dP/dV': group['dP/dV'].values.tolist(),
        'dF/dP': group['dF/dP'].values.tolist()
    }

    breath_setting = align_breath(group, breath_df, file)

    mongo_record = {
        '_id': str(int(group.patient_ID.head(1))) + '/' + str(group.breath.min()) + '/' + str(group.date_time.min())
               + '/' + str(start_time),
        'patient_id': int(group.patient_ID.head(1)),
        'file': group.file.head(1).values[0],
        'breath_num': int(group.breath.min()),
        'date_time': group.date_time.min().timestamp(),
        'loc': [group.date_time.min().timestamp(), int(group.patient_ID.head(1))],
        'breath_settings': breath_setting,
        'breath_raw': raw_dict,
        'breath_character': breath_dict,
        'breath_derivative': calc_inner_df.to_dict(orient = 'list')
    }

    if mongo_record['breath_character']['elapse_time'] > 128:
        mongo_record = WA.analyze_breath(mongo_record)
    else:
        input_log.update_one({'_id': file['_id']}, {
            '$addToSet': {'warnings': 'breath_too_short_warning',
                          'breath_too_short_warning': mongo_record['breath_num']}})

    return mongo_record


def get_waveform_and_breath(file):
    print(file)
    breath_df = get_breath_data(file)
    wave_df = get_waveform_data(file)

    bulk_ops = bulk.BulkOperationBuilder(breath_col, ordered = False)

    for name, group in wave_df.groupby('breath', sort = False):
        input_log.update_one({'_id': file['_id'], 'errors': 'insert_error'},
                             {'$unset': {'errors': '', 'insert_error': '', 'other_error': ''}})
        try:
            bulk_ops.insert(waveform_data_entry(group, breath_df, file))
        except Exception as e:
            print('Insert Error', e)
            input_log.update_one({'_id': file['_id']},
                                 {'$addToSet': {'errors': 'insert_error', 'insert_error': str(e)}})

    try:
        bulk_ops.execute()
    except errors.BulkWriteError as bwe:
        for items in bwe.details['writeErrors']:
            if items['code'] != 11000:
                print('BulkWrite', items['errmsg'])
                input_log.update_one({'_id': file['_id']},
                                     {'$addToSet': {'errors': 'bulk_write_error', 'bulk_error': 'bulk error'}})
            else:
                pass
    except errors.InvalidDocument as e:
        print('InvalidDoc', e)
        input_log.update_one({'_id': file['_id']},
                             {'$addToSet': {'errors': 'invalid_doc_error', 'invalid_doc_error': str(e)}})
    except Exception as e:
        input_log.update_one({'_id': file['_id']}, {'$addToSet': {'errors': 'other_error', 'other_error': str(e)}})
        print('Error1', e)

    if input_log.find({'_id': file['_id'], 'errors': {'$exists': 1}}, {'_id': 1}).count() < 1:
        try:
            input_log.update_one({'_id': file['_id']}, {'$set': {'loaded': 1}})
        except:
            pass

        try:
            input_log.update_one({'_id': file['match_file']}, {'$set': {'loaded': 1, 'crossed': 1}})
        except:
            pass
