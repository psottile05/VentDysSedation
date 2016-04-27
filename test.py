import pandas as pd
import dask.dataframe as dd
from pymongo import MongoClient

client = MongoClient()
db = client.VentDB
input_log = db.input_log
breath_col = db.breath_collection
RN_col = db.breath_collection
lab_col = db.Lab_collection

if __name__ == '__main__':
    # results = breath_col.find({}, {'breath_raw': 0, 'max_min_raw': 0, 'breath_derivative': 0, 'loc': 0})
    # print(results.count())
    # df = pd.io.json.json_normalize(results)
    #
    # df['date_time'] = pd.to_datetime(df['date_time'], unit = 's').dt.tz_localize('UTC').dt.tz_convert(
    #     'US/Mountain').dt.tz_localize(None)
    # df.drop_duplicates(subset = 'date_time', keep = 'last', inplace = True)
    # df = df.set_index(['patient_id', 'date_time'], drop = False, verify_integrity=True)
    # df.sort_index(inplace = True)
    # df = df.set_index(['patient_id'], drop = False)
    # print(df.shape)
    # df.to_hdf('c:\\Research_data\\Analysis\\pandas_breath_dump_4_26_16__2.h5', key = 'breath_collection',
    # format = 'table',
    #           append = False)
    #
    # ddf = dd.from_pandas(df, npartitions = 1000)
    # ddf.to_hdf('c:\\Research_data\\Analysis\\dask_breath_dump_4_26_16__2.h5', key = 'breath_collection')

    results = RN_col.find({})
    print(results.count)
    df = pd.io.json.json_normalize(results)
    df['date_time'] = pd.to_datetime(df['date_time'], unit = 's')
    df = df.set_index(['patient_id', 'date_time'], drop = False, verify_integrity = True)
    df.sort_index(inplace = True)
    df = df.set_index(['patient_id'], drop = False)
    print(df.shape)
    df.to_hdf('c:\\Research_data\\Analysis\\pandas_rn_dump_4_26_16.h5', key = 'rn_collection', format = 'table',
              append = False)

    ddf = dd.from_pandas(df, npartitions = 1000)
    ddf.to_hdf('c:\\Research_data\\Analysis\\dask_rn_dump_4_26_16.h5', key = 'rn_collection')

    results = lab_col.find({})
    print(results.count)
    df = pd.io.json.json_normalize(results)
    df['date_time'] = pd.to_datetime(df['date_time'], unit = 's')
    df = df.set_index(['patient_id', 'date_time'], drop = False, verify_integrity = True)
    df.sort_index(inplace = True)
    df = df.set_index(['patient_id'], drop = False)
    print(df.shape)
    df.to_hdf('c:\\Research_data\\Analysis\\pandas_lab_dump_4_26_16.h5', key = 'rn_collection', format = 'table',
              append = False)

    ddf = dd.from_pandas(df, npartitions = 1000)
    ddf.to_hdf('c:\\Research_data\\Analysis\\dask_lab_dump_4_26_16.h5', key = 'rn_collection')
