#!/usr/bin/env python3

from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.ngx3fy
# specify a collection
collection = db.dp2

data = os.listdir('/workspace/ds2002-dp2/data/')

imported = 0
not_imported = 0


directory = "data"
for filename in os.listdir(directory):
  with open(os.path.join(directory, filename)) as f:
    try:
      json_file = json.load(f)
      print(filename, "passed through")
    except Exception as e:
      print(filename, "did not pass, ", e)
      not_imported += len(json_file)
    if isinstance(json_file, list):
        try:
            collection.insert_many(json_file)
            imported += len(json_file) 
        except Exception as e:
            print(e)
    else:
        try:
            collection.insert_one(json_file)
        except Exception as e:
            print(e)
            

print(imported)
print(not_imported)