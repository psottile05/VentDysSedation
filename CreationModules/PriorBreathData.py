from pymongo import MongoClient

client = MongoClient()
db = client.VentDB

input_log = db.input_log
breath_col = db.breath_collection


def update_breath(current_breath):
    # current_breath_list = breath_col.find({'next_breath_data': {'$exists': 0}},
    #                                      {'patient_id': 1, 'file': 1, 'breath_num': 1, 'breath_character': 1})

    #for current_breath in current_breath_list:
        next_breath = breath_col.find_one({'patient_id': current_breath['patient_id'],
                                           'file': current_breath['file'],
                                           'breath_num': current_breath['breath_num'] + 1},
                                          {'_id': 0, 'breath_character': 1})

    if not isinstance(next_breath, type(None)):
            breath_col.find_one_and_update({'patient_id': current_breath['patient_id'],
                                            'file': current_breath['file'],
                                            'breath_num': current_breath['breath_num']},
                                           {'$set': {'next_breath_data': next_breath['breath_character']}}
                                           )
