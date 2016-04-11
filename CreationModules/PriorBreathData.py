from pymongo import MongoClient
from ipyparallel import Client

client = MongoClient()
db = client.VentDB

input_log = db.input_log
breath_col = db.breath_collection

ipclient = Client()
ipview = ipclient.direct_view()

ipview.parallel()


def update_breath(breath_list):
    for current_breath in breath_list:
        next_breath = breath_col.find_one({'patient_id': current_breath['patient_id'],
                                           'file': current_breath['file'],
                                           'breath_num': current_breath['breath_num'] + 1},
                                          {'_id': 0, 'breath_character': 1})

        if not isinstance(next_breath, type(None)):
            try:
                breath_col.find_one_and_update({'patient_id': current_breath['patient_id'],
                                                'file': current_breath['file'],
                                                'breath_num': current_breath['breath_num']},
                                               {'$set': {'next_breath_data': next_breath['breath_character']}}
                                               )
            except Exception as e:
                print('next update error: ', str(e))
                input_log.update_one({'_id': current_breath['_id']},
                                     {'$addToSet': {'errors': 'next_breath_update_error', 'update_error': str(e)}})
        else:
            pass
