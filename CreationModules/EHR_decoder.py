import itertools
import re
from pathlib import Path
import numpy as np
import pandas as pd
from pymongo import MongoClient

client = MongoClient()
db = client.VentDB
lab_db = db.Lab_collection
RN_db = db.RN_collection
input_log = db.input_log


def data_analysis(fileName):
    raw_data = []

    data = open(fileName, 'r+')
    text = data.read()
    entries = re.split('\n\n', text)

    for items in entries:
        raw_data.append(items.strip('\n '))

    search_items = {'DateTime': {'head': '',
                                 'pat': '(\d\d/\d\d/\d\d\n \d\d\d\d)|(\d\d/\d\d/\d\d\n \d\d:\d\d:\d\d)|('
                                        '\d\d/\d\d/\d\d\n \d\d:\d\d:\d\d:\d\d)'},
                    'BP': {'head': 'BP(?! )', 'pat': '\d\d\d?/\d\d?\d?'},
                    'MAP': {'head': '(?!= A-line )MAP', 'pat': '\d\d\d?'},
                    'Resp': {'head': 'Resp(?![a-z ])', 'pat': '\d\d?'},
                    'Pulse': {'head': 'Pulse', 'pat': '\d\d?\d?'},
                    'A-line': {'head': 'A-line(?! MAP)', 'pat': '\d\d\d?/\d\d?\d?'},
                    'A-line MAP': {'head': 'A-line MAP', 'pat': '\d\d\d?'},
                    'CVP': {'head': 'CVP ', 'pat': '\d\d?'},
                    'SpO2': {'head': '(?!=-)SpO2(?!-)', 'pat': '\d\d+'},
                    'FiO2': {'head': 'FiO2', 'pat': '(\d\d+)|(\d\d\d)'},
                    'Vent Mode': {'head': 'Vent Mode',
                                  'pat': '(PRVC)|((?<!;)CPAP)|(PSV(?!;))|(PSV;CPAP)|(APV(?!;))|((?<!;)CMV)|(APV;CMV)'},
                    'Set Vt': {'head': 'Set Vt', 'pat': '\d\d\d'},
                    'Set RR': {'head': 'Set RR', 'pat': '\d\d?'},
                    'Position': {'head': 'Repositioned', 'pat': '\w+'},
                    'RASS': {'head': 'RASS Sedation Scale', 'pat': '.\d'},
                    'TOF': {'head': 'Twitches', 'pat': '\d'},
                    'PEEP': {'head': 'PEEP', 'pat': '\d\d?'},
                    'Plat': {'head': 'Static Pressure', 'pat': '\d+'},
                    'iNO': {'head': 'NO(?!2)', 'pat': '(\d\d)|(0\.\d)'},
                    'CPOT Tot': {'head': 'CPOT Total', 'pat': '\d+'},
                    'CPOT Vent': {'head': 'Ventilator Compliance', 'pat': '\d+'}}

    found_items = {'DateTime': {'rows': 0, 'values': []},
                   'BP': {'rows': 0, 'values': []},
                   'MAP': {'rows': 0, 'values': []},
                   'Pulse': {'rows': 0, 'values': []},
                   'Resp': {'rows': 0, 'values': []},
                   'A-line': {'rows': 0, 'values': []},
                   'A-line MAP': {'rows': 0, 'values': []},
                   'CVP': {'rows': 0, 'values': []},
                   'SpO2': {'rows': 0, 'values': []},
                   'FiO2': {'rows': 0, 'values': []},
                   'Vent Mode': {'rows': 0, 'values': []},
                   'Set Vt': {'rows': 0, 'values': []},
                   'Set RR': {'rows': 0, 'values': []},
                   'Position': {'rows': 0, 'values': []},
                   'RASS': {'rows': 0, 'values': []},
                   'TOF': {'rows': 0, 'values': []},
                   'PEEP': {'rows': 0, 'values': []},
                   'Plat': {'rows': 0, 'values': []},
                   'iNO': {'rows': 0, 'values': []},
                   'CPOT Tot': {'rows': 0, 'values': []},
                   'CPOT Vent': {'rows': 0, 'values': []}}

    final_data = {
        'DateTime': ['BP', 'MAP', 'Pulse', 'A-line', 'A-line MAP', 'CVP', 'SpO2', 'FiO2', 'Resp', 'Vent Mode', 'Set Vt',
                     'Set RR', 'Position', 'RASS', 'TOF', 'PEEP', 'Plat', 'iNO', 'CPOT Tot', 'CPOT Vent']}

    def date_cleaner(text):
        clean_up = text.split('\n')
        text = clean_up[0] + clean_up[1]

        if ':' in text:
            clean_up = text.split(':')
            text = clean_up[0] + clean_up[1]
        return text

    count = 0
    for items in raw_data:
        if items == 'Vitals':
            count += 1

    # print(count)

    for items in raw_data:
        results = re.match(search_items['DateTime']['pat'], items)

        if results:
            text = results.group(0)
            text = date_cleaner(text)
            found_items['DateTime']['rows'] += 1
            found_items['DateTime']['values'].append(text)

    for items in search_items:
        if items != 'DateTime':
            for i in range(0, len(raw_data)):
                # Match to Desired Heading
                results = re.match(search_items[items]['head'], raw_data[i])

                if results:
                    # if positive patch, set up blank variables
                    temp = []  # obtain surrounding data
                    no_date = True
                    k = i
                    dates = []

                    # update number of hits for that heading
                    found_items[items]['rows'] += 1

                    # match desired data and blanks to keep spacing in temp list
                    for k in range(i, i + 19):
                        sub_results = re.match(search_items[items]['pat'], raw_data[k])

                        if sub_results:
                            temp.append(sub_results.group(0))
                        else:
                            temp.append(raw_data[k])

                    # special management of SPO2, Resp, HR because of Epic
                    to_pop = []
                    if items == 'SpO2':
                        for y in range(0, len(temp) - 1):
                            if temp[y].isdecimal() and int(temp[y]) < 90:
                                to_pop.append(y - 1)
                        for values in to_pop[::-1]:
                            temp.pop(values)
                    elif items == 'Resp':
                        for y in range(0, len(temp) - 1):
                            if temp[y].isdecimal() and int(temp[y]) > 30:
                                to_pop.append(y - 1)
                        for values in to_pop[::-1]:
                            temp.pop(values)
                    elif items == 'Pulse':
                        for y in range(0, len(temp) - 1):
                            if temp[y].isdecimal() and int(temp[y]) < 60:
                                to_pop.append(y - 1)
                        for values in to_pop[::-1]:
                            temp.pop(values)

                    # find most recent set of date data to match with
                    while no_date:
                        date_results = re.match(search_items['DateTime']['pat'], raw_data[k])

                        if date_results:
                            for j in range(k, k - 12, -1):
                                more_dates = re.match(search_items['DateTime']['pat'], raw_data[j])
                                if more_dates:
                                    dates.append(date_cleaner(more_dates.group(0)))
                            no_date = False

                        k -= 1

                    # print('\t', dates[::-1], '\n', temp)

                    # zip dates and values into tuple
                    zipped = zip(dates[::-1], temp[1:])
                    found_items[items]['values'].append(list(zipped))

    for items in found_items:
        if items != 'DateTime':
            lists = list(itertools.chain(*found_items[items]['values']))
            found_items[items]['values'] = lists

            # for items in found_items:
            # print(items, len(found_items[items]['values']), found_items[items]['rows'] * 5)
            # print (found_items[items]['values'])

    dicts = []
    for items in final_data['DateTime']:
        for values in found_items[items]['values']:
            dicts.append({items: values[1], 'DateTime': values[0]})

    df = pd.DataFrame.from_dict(found_items['DateTime']['values'])
    df.rename(columns = {0: 'date_time'}, inplace = True)
    df['date_time'] = pd.to_datetime(df['date_time'], infer_datetime_format = True)
    df.drop_duplicates(subset = 'date_time', inplace = True, keep = 'first')
    df.set_index('date_time', inplace = True, drop = True, verify_integrity = True)

    temp_df = pd.DataFrame(dicts)
    temp_df.replace(to_replace = '', value = np.nan, inplace = True)
    temp_df.replace(to_replace = '--', value = np.nan, inplace = True)
    temp_df['DateTime'] = pd.to_datetime(temp_df['DateTime'], infer_datetime_format = True)
    temp_df.set_index('DateTime', inplace = True, drop = True)
    temp_df.dropna(inplace = True, axis = 0, how = 'all')
    temp_df = pd.to_numeric(temp_df, errors = 'ignore')

    cols = temp_df.columns
    for col in cols:
        temp = temp_df[col]
        temp.dropna(inplace = True)

        df = df.join(temp, how = 'left')

    df.dropna(inplace = True, how = 'all')

    try:
        df['SBP'] = df['BP'].str.split('/', n = 1).str.get(0)
        df['DBP'] = df['BP'].str.split('/', n = 1).str.get(1)
        df.drop(['BP'], axis = 1, inplace = True)
    except:
        pass

    try:
        df['A_SBP'] = df['A-line'].str.split('/', n = 1).str.get(0)
        df['A_DBP'] = df['A-line'].str.split('/', n = 1).str.get(1)
        df.drop(['A-line'], axis = 1, inplace = True)
    except:
        pass

    return df


def lab_analysis(fileName):
    class labTypes:
        def __init__(self, dateTime, labName, labValue, labGroup):
            self.dateTime = dateTime
            self.labName = labName
            self.labValue = labValue
            self.labGroup = labGroup

        def make_tuple(self):
            return self.dateTime, self.labGroup, self.labName, self.labValue

    file = open(fileName)
    fileLines = file.readlines()
    file.close()

    labCollection = []

    for lines in fileLines:
        if re.match(r'\d\d?/\d\d?/\d\d\d\d \d\d:\d\d', lines):
            dateTime = lines.strip()

        if re.search(r'\w: \d', lines):
            lineInfo = lines.split(": ")

            labName = lineInfo[0].strip()

            if labName in ['Alanine Aminotransferase', 'Aspartate Aminotransferase', 'Bilirubin Total', 'ALBUMIN',
                           'Alk Phos, Serum/Plasma', 'Protein Total, Serum/Plasma', 'Bilirubin, Direct',
                           'Bilirubin, Indirect']:
                labGroup = 'LFT'
            elif labName in ['Sodium, Serum/Plasma', 'Potassium, Serum/Plasma', 'Chloride, Serum/Plasma',
                             'Carbon Dioxide', 'GLUCOSE, RANDOM, SERUM/PLASMA', 'Blood Urea Nitrogen',
                             'CREATININE, SERUM/PLASMA', 'Calcium, Serum/Plasma', 'ANION GAP',
                             'MAGNESIUM SERUM', 'PHOSPHORUS, SERUM/PLASMA']:
                labGroup = 'Chemistry'
            elif labName in ['WHITE BLOOD CELL COUNT', 'HEMOGLOBIN', 'PLATELET COUNT']:
                labGroup = 'CBC'
            elif labName in ['LACTATE WHOLE BLOOD VENOUS', 'LACTATE WHOLE BLOOD ARTERIAL']:
                labGroup = 'Lactate'
            elif labName in ['Prothrombin Time', 'INTERNATIONAL NORMALIZED RATIO']:
                labGroup = 'Coags'
            elif labName in ['pH ARTERIAL', 'Pco2 Arterial', 'PO2 Arterial', 'BICARBONATE ARTERIAL',
                             'MEASURED O2SAT ARTERIAL', 'FiO2 ARTERIAL']:
                labGroup = 'ABG'
            elif labName in ['pH Venous', 'Pco2 Venous', 'PO2 Venous', 'Bicarbonate Venous', 'O2SAT Venous Measured']:
                labGroup = 'VBG'
            else:
                labGroup = 'Other'

            labName = ''.join(labName.split(',')).lower()

            labValue = lineInfo[1].split(" (")[0].strip()

            labCollection.append(labTypes(dateTime, labName, labValue, labGroup))

    df = pd.DataFrame.from_records([items.make_tuple() for items in labCollection], columns = ['date_time', 'group',
                                                                                               'lab', 'value'])
    df['value'] = pd.to_numeric(df['value'], errors = 'coerce')
    df['date_time'] = pd.to_datetime(df['date_time'], infer_datetime_format = True, errors = 'coerce')

    return df


def load_EHR_data(path, patients):
    print(path, patients)

    if ('RN' in path) and 'edit' not in path:
        try:
            rn_df = data_analysis(path)
            rt_df = data_analysis(Path(path).parent.joinpath('RT Data.txt').as_posix())
            tot_df = rn_df.combine_first(rt_df)
        except Exception as e:
            error = {path, e}
            print(error)
            tot_df = pd.DataFrame()

        tot_df.reset_index(inplace = True)
        tot_df.rename(columns = {'index': 'date_time'}, inplace = True)
        tot_df['patientID'] = patients
        RN_db.insert_many(tot_df.to_dict(orient = 'records'), ordered = False)
        input_log.update_one({'_id': Path(path).parent.joinpath('RT Data.txt').as_posix()}, {'$set': {'loaded': 1}})

    elif ('Lab' in path) and 'edit' not in path:
        try:
            df = lab_analysis(path)
        except Exception as e:
            error = {path, e}
            print(error)

        df['patientID'] = patients
        lab_db.insert_many(df.to_dict(orient = 'records'), ordered = False)

    elif 'RT' in path:
        pass
    else:
        print(path, 'unknown file type')

        # input_log.update_one({'_id': path}, {'$set': {'loaded': 1}})
