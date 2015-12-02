import pandas as pd
import numpy as np
import scipy as sp
import scipy.signal as sig
import numba


def find_min():
    pass


def find_max():
    pass


# Insp Min/Max
# Exp Min/Max

def analyze_breath(mongo_record):
    print(mongo_record.keys())
    raw_df = pd.DataFrame(mongo_record['breath_raw'])
    print(raw_df.columns)

    return mongo_record
