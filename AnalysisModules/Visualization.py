import pandas as pd
from pymongo import MongoClient
from bokeh.plotting import figure, output_notebook, show, vplot

client = MongoClient()
db = client.VentDB
breath_db = db.breath_collection


def get_breaths(limits):
    results = breath_db.find().limit(limits)
    return results


def breath_viz(id):
    breath = list(breath_db.find({'_id': id}, {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]
    breath_start = breath['breath_raw']['time'][0]
    breath_end = breath['breath_raw']['time'][-1]

    try:

        pre_breath = list(breath_db.find({'file': breath['file'], 'breath_num': breath['breath_num'] - 1},
                                         {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]
    except IndexError:
        pre_breath = {
            'breath_raw': {'flow': [], 'sm_dV/dTT': [], 'vol': [], 'dF/dT': [], 'sm_vol': [], 'breath': [], 'time': [],
                           'sm_dF/dT': [], 'sm_flow': [], 'status': [], 'sm_dF/dTT': [], 'sm_dV/dT': [], 'dP/dV': [],
                           'paw': [], 'sm_paw': [], 'sm_dP/dT': [], 'dF/dV': [], 'dP/dT': [], 'dV/dT': [], 'dF/dP': [],
                           'sm_dP/dTT': []}}

    try:
        post_breath = list(breath_db.find({'file': breath['file'], 'breath_num': breath['breath_num'] + 1},
                                          {'file': 1, 'breath_num': 1, 'breath_raw': 1}))[0]
    except IndexError:
        post_breath = {
            'breath_raw': {'flow': [], 'sm_dV/dTT': [], 'vol': [], 'dF/dT': [], 'sm_vol': [], 'breath': [], 'time': [],
                           'sm_dF/dT': [], 'sm_flow': [], 'status': [], 'sm_dF/dTT': [], 'sm_dV/dT': [], 'dP/dV': [],
                           'paw': [], 'sm_paw': [], 'sm_dP/dT': [], 'dF/dV': [], 'dP/dT': [], 'dV/dT': [], 'dF/dP': [],
                           'sm_dP/dTT': []}}

    df = pd.concat([pd.DataFrame(pre_breath['breath_raw']), pd.DataFrame(breath['breath_raw']),
                    pd.DataFrame(post_breath['breath_raw'])])

    return df, breath_start, breath_end


def make_plot(df, breath_start, breath_end):
    p1, p2, p3 = [figure(plot_height = 250, toolbar_location = 'right') for x in range(3)]
    p1.line(df['time'], df['sm_flow'], color = 'firebrick', name = 'Flow')
    p1.line([breath_start, breath_start], [-40, 40], color = 'green')
    p1.line([breath_end, breath_end], [-40, 40], color = 'green')
    p1.yaxis.axis_label = 'Flow'

    p2.line(df['time'], df['sm_paw'], color = 'navy', name = 'Paw')
    p2.line([breath_start, breath_start], [0, 40], color = 'green')
    p2.line([breath_end, breath_end], [0, 40], color = 'green')
    p2.yaxis.axis_label = 'Paw'

    p3.line(df['time'], df['sm_vol'], color = 'olive', name = 'Volume')
    p3.line([breath_start, breath_start], [0, 400], color = 'green')
    p3.line([breath_end, breath_end], [0, 400], color = 'green')
    p3.yaxis.axis_label = 'Volume'

    p = vplot(p1, p2, p3)
    return p


def update_database(_id, labels):
    analysis = {'norm': 1 if 'Normal' in labels else 0,
                'ds': 1 if 'DoubleStacked' in labels else 0,
                'pds': 1 if 'PostDoubleStacked' in labels else 0,
                'pfl': 1 if 'PressureFlowLimited' in labels else 0,
                'fa': 1 if 'FlowAbnormal' in labels else 0,
                'pvt': 1 if 'PrematureVentTermination' in labels else 0,
                'ie': 1 if 'IneffectiveTrigger' in labels else 0,
                'garb': 1 if 'Garbage' in labels else 0,
                }

    breath_db.update({'_id': _id}, {'$set': {'validation': analysis}})

    return
