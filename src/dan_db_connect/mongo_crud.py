from typing import Optional
import pandas as pd
import json
from pymongo import MongoClient


class mongodb_operation():
    def __init__(self, client_uri: str, database_name: str, collection_name: Optional[str] = None): 
        self.client_uri = client_uri
        self.database_name = database_name
        self.collection_name = collection_name  
          
    def create_client(self):
        client = MongoClient(self.client_uri)
        return client
       
    def create_database(self):
        client = self.create_client()
        database = client[self.database_name] 
        return database
    
    def create_collection(self, collection):
        database = self.create_database()
        collection = database[collection]
        return collection
    
    def insert_record(self, record, collection_name):
        if type(record) == list:
            for data in record:
                if type(data) != dict:
                    raise TypeError("record must be a dictionary")
            collection = self.create_collection(collection_name)
            collection.insert_many(record)
        else:
            type(record) == dict
            collection = self.create_collection(collection_name)
            collection.insert_one(record)

    def bulk_insert(self, datafile: str, collection_name: Optional[str] = None):
        self.path = datafile
        
        if self.path.endswith(".csv"):
            data = pd.read_csv(self.path, encoding="utf-8")
        elif self.path.endswith(".xlsx"):
            data = pd.read_excel(self.path, encoding="utf-8")
        
        data_json = json.loads(data.to_json(orient="records"))
        collection = self.create_collection(collection_name)
        collection.insert_many(data_json)
        
    def find(self, query: dict, collection_name: Optional[str] = None):
        collection = self.create_collection(collection_name)
        return collection.find(query)

    def delete(self, query: dict, collection_name: Optional[str] = None):
        collection = self.create_collection(collection_name)
        collection.delete_one(query)

    def update(self, query: dict, update_values: dict, collection_name: Optional[str] = None):
        collection = self.create_collection(collection_name)
        collection.update_one(query, {'$set': update_values})
