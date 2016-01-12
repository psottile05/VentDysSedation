import numpy as np
import pandas as pd
import numba

from bokeh.charts import Scatter, Line, output_file, show
from bokeh.models import LinearAxis, Range1d
from bokeh.io import vform
from bokeh.models.widgets import CheckboxButtonGroup, Button


@numba.jit(nopython = True, nogil = True, cache = True)
def count_breath(flow):
    index = 0
    count = 0
    direction = 0
    sample_rate = 0.06511627

    n = len(flow)
    breath = np.empty_like(flow)
    status = np.empty_like(flow)
    vol = np.empty_like(flow)
    time = np.empty_like(flow)

    for rindex, x in np.ndenumerate(flow):
        if np.isnan(x):
            count += 0.5
        elif x < 2:
            direction = 0
        elif x >= 2:
            direction = 1

        vol[index] = x * sample_rate
        breath[index] = count
        status[index] = direction
        time[index] = int(count * sample_rate * 1000)

        index += 1

    return breath, status, vol


df = pd.read_csv(r'c:\Research_data\DH_data\test-840-1.txt', engine = 'python', skiprows = 2, skipfooter = 2,
                 names = ['flow', 'paw', 'other'])

df.reset_index(drop = False, inplace = True)
df.drop(labels = 'other', axis = 1, inplace = True)
df.flow = pd.to_numeric(df.flow, errors = 'coerce')
df.paw = pd.to_numeric(df.paw, errors = 'coerce')

df['breath'], df['status'], df['vol'] = count_breath(df.flow.values)
df.dropna(axis = 0, how = 'all', subset = ['flow'], inplace = True)

df.vol = df.groupby('breath')['vol'].cumsum()

p = Line(df, x = 'index', y = ['flow', 'paw', 'vol', 'breath', 'status'], color = 'red')
p.extra_y_ranges = {'flow': Range1d(start = 0, end = 30)}
p.add_layout(LinearAxis(y_range_name = 'flow'), 'left')

check_button = CheckboxButtonGroup(labels = ['Double Stacked', 'Flow Limited', 'Ineffective Trigger'],
                                   active = [0, 0, 0])
next_button = Button(label = 'Next', type = 'success')
next_button.on_click(print('ok'))
output_file('test.html')

show(vform(p, check_button, next_button))


