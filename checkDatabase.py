from pymongo import MongoClient

client = MongoClient()
db = client.VentDB
input_log = db.input_log
breath_col = db.breath_collection

print(breath_col.find().count())
