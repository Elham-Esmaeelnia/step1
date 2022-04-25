#Task2-Mongodb-section3
import requests
import time
import pymongo
import json
from pymongo import MongoClient
#client = MongoClient('localhost')
#print(client)
'''
db = client.clients
db.clients.count()
clients = db.clients
clients.find()
'''
data_array = []
data_uuid = []
max_user_num = 100000
j=1
while j < max_user_num:
    try:
        response = requests.get("https://randomuser.me/api/?results=5000")
        data = response.json()
        for i in data['results']:
            user_uuid = i["login"]["uuid"]
            if user_uuid in data_uuid:
                j=j
            else:
                data_array.append(i)
                data_uuid.append(user_uuid)
                j+=1
                print('user by this uuid added to mongodb:' + i["login"]["uuid"])
        print('Please Wait...')        
        time.sleep(30)    
    except:
        print('Error accurred!')
        print('Please Wait...')
print(len(data_uuid)) 
# add to mongodb
  
try:
    conn = MongoClient("mongodb://mongoAdmin:changeMe@localhost:27017/")
    print("Connected successfully!!!")
except:  
    print("Could not connect to MongoDB")
  
# database
db = conn.admin
  
# Created or Switched to collection names: mytask2_section3
collection = db.mytask2_section3
   
# Insert Data
for rec in data_array:
  rec_id1 = collection.insert_one(rec)
  print("Data inserted with record ids",rec_id1)
  
# Printing the data inserted
cursor = collection.find()
for record in cursor:
    print(record)                   