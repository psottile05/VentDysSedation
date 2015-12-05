import pandas as pd
import numpy as np
import scipy as sp
import scipy.signal as sig
import numba


def second_deriv(raw_df):
    raw_df['sm_dF/dT'] = raw_df['sm_flow'].diff()
    raw_df['sm_dP/dT'] = raw_df['sm_paw'].diff()
    raw_df['sm_dV/dT'] = raw_df['sm_vol'].diff()

    raw_df['sm_dF/dTT'] = raw_df['sm_dF/dT'].diff()
    raw_df['sm_dP/dTT'] = raw_df['sm_dP/dT'].diff()
    raw_df['sm_dV/dTT'] = raw_df['sm_dV/dT'].diff()

    return raw_df

def find_min():
    pass


def find_max():
    pass


# Insp Min/Max
# Exp Min/Max

def analyze_breath(mongo_record):
    raw_df = pd.DataFrame(mongo_record['breath_raw'])
    raw_df = second_deriv(raw_df)
    breath_raw = raw_df.to_dict(orient = 'list')
    mongo_record['breath_raw'] = breath_raw
    return mongo_record
