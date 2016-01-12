import pandas as pd
from pymongo import MongoClient
from bokeh.plotting import figure, output_notebook, show


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

    return df


def make_plot(df):
    output_notebook()

    p = figure(plot_height = 800)
    p.line([1, 2, 3], [1, 2, 3])

    return p
