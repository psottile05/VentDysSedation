import pandas as pd
import dask.dataframe as dd
from pymongo import MongoClient
import numpy as np

client = MongoClient()
db = client.VentDB
input_log = db.input_log
breath_col = db.breath_collection
RN_col = db.RN_collection
lab_col = db.Lab_collection

if __name__ == '__main__':
    unique_id = breath_col.find({}, {'patient_id': 1}).distinct('patient_id')
    print(unique_id)
    unique_id = [100]
    for ids in unique_id:
        results = breath_col.find({'patient_id': ids}, {'breath_raw': 0, 'max_min_raw': 0, 'breath_derivative': 0,
                                                        'file': 0, 'breath_settings.file': 0, 'loc': 0})
        print(ids)
        print(results.count())
        df = pd.io.json.json_normalize(results)

        df['date_time'] = pd.to_datetime(df['date_time'], unit='s').dt.tz_localize('UTC').dt.tz_convert(
            'US/Mountain').dt.tz_localize(None)
        df.drop_duplicates(subset='date_time', keep='last', inplace=True)
        df = df.set_index(['patient_id', 'date_time'], drop=False, verify_integrity=True)
        df.sort_index(inplace=True)
        df = df.set_index(['patient_id'], drop=False)
        print(df.shape)
        print(df.dtypes[df.dtypes == np.object])
        df.to_hdf('c:\\Research_data\\Analysis\\' + str(ids) + '_pandas_breath_dump_5_1_16.h5', key='breath_collection',
                  format='table', append=False)

        # ddf = dd.from_pandas(df, npartitions = 1000)
        # ddf.to_hdf('c:\\Research_data\\Analysis\\'+str(ids)+'_dask_breath_dump_5_1_16.h5', key = 'breath_collection')

    unique_id = breath_col.find({}, {'patient_id': 1}).distinct('patient_id')
    tot_df = pd.DataFrame()
    for ids in unique_id:
        print(ids)
        df = pd.read_hdf('c:\\Research_data\\Analysis\\' + str(ids) + '_pandas_breath_dump_5_1_16.h5',
                         key='breath_collection',
                         format='table')
        tot_df = pd.concat([tot_df, df])
    print(tot_df.shape)
    tot_df.to_hdf('c:\\Research_data\\Analysis\\pandas_breath_dump_5_1_16.h5', key='breath_collection',
                  format='table', append=False)

    # ddf = dd.from_pandas(tot_df, npartitions=1000)
    # ddf.to_hdf('c:\\Research_data\\Analysis\\dask_breath_dump_5_1_16.h5', key = 'breath_collection')

    results = RN_col.find({}, {'_id': 0})
    print('RN', results.count)
    df = pd.io.json.json_normalize(list(results))
    df['date_time'] = pd.to_datetime(df['date_time'])
    df.drop(['Position', 'Vent Mode'], inplace = True, axis = 1)
    print(df.dtypes)

    df = df.set_index(['patientID', 'date_time'], drop = False)
    df.sort_index(inplace = True)
    df = df.set_index(['patientID'], drop = False)
    print(df.shape)
    df.to_hdf('c:\\Research_data\\Analysis\\pandas_rn_dump_5_1_16.h5', key='rn_collection', format='table',
              append = False)

    ddf = dd.from_pandas(df, npartitions = 1000)
    ddf.to_hdf('c:\\Research_data\\Analysis\\dask_rn_dump_5_1_16.h5', key='rn_collection')

    results = lab_col.find({}, {'_id': 0})
    print('Lab', results.count)
    df = pd.io.json.json_normalize(list(results))
    df['date_time'] = pd.to_datetime(df['date_time'])
    print(df.dtypes)
    df = df.set_index(['patientID', 'date_time'], drop = False)
    df.sort_index(inplace = True)
    df = df.set_index(['patientID'], drop = False)
    print(df.shape)
    df.to_hdf('c:\\Research_data\\Analysis\\pandas_lab_dump_5_1_16.h5', key='lab_collection', format='table',
              append = False)

    ddf = dd.from_pandas(df, npartitions = 1000)
    ddf.to_hdf('c:\\Research_data\\Analysis\\dask_lab_dump_5_1_16.h5', key='lab_collection')
