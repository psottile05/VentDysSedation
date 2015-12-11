import pandas as pd
from bokeh.charts import Scatter, Line, output_file, show
from bokeh.models import LinearAxis, Range1d
from bokeh.io import vform
from bokeh.models.widgets import CheckboxButtonGroup, Button

df = pd.read_csv(r'c:\Users\sottilep\Desktop\test-840-2.txt', engine = 'python', skiprows = 2, skipfooter = 2,
                 names = ['flow', 'paw', 'other'])
df.reset_index(drop = False, inplace = True)
df = df.convert_objects(convert_numeric = True)
df.dropna(axis = 0, how = 'all', inplace = True)

p = Line(df, x = 'index', y = ['flow', 'paw'], color = 'red')
p.extra_y_ranges = {'flow': Range1d(start = 0, end = 30)}
p.add_layout(LinearAxis(y_range_name = 'flow'), 'left')

check_button = CheckboxButtonGroup(labels = ['Double Stacked', 'Flow Limited', 'Ineffective Trigger'],
                                   active = [0, 0, 0])
next_button = Button(label = 'Next', type = 'success')
next_button.on_click(print('ok'))
output_file('test.html')

show(vform(p, check_button, next_button))
