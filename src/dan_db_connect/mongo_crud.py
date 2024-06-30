import pandas as pd
import json
from pymongo import MongoClient


class mongodb_operation():
    def __init__(self, client_uri: str, database_name: str, collection_name: str = None): 
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
    

    def create_collection(self, collection=None):
        if collection is None:
            collection = self.collection_name
        database = self.create_database()
        collection = database[collection]
        return collection
    

    def insert_record(self, record, collection_name=None):
        collection = self.create_collection(collection_name)
        if isinstance(record, list):
            for data in record:
                if not isinstance(data, dict):
                    raise TypeError("record must be a dictionary")
            collection.insert_many(record)
        elif isinstance(record, dict):
            collection.insert_one(record)
        else:
            raise TypeError("record must be a dictionary or list of dictionaries")
        

    def bulk_insert(self, datafile: str, collection_name: str = None):
        self.path = datafile
        if self.path.endswith(".csv"):
            data = pd.read_csv(self.path, encoding="utf-8")
        elif self.path.endswith(".xlsx"):
            data = pd.read_excel(self.path, encoding="utf-8")
        else:
            raise ValueError("File format not supported. Please provide a .csv or .xlsx file.")

        data_json = json.loads(data.to_json(orient="records"))
        collection = self.create_collection(collection_name)
        collection.insert_many(data_json)
        

    def find_records(self, query, collection_name=None):
        collection = self.create_collection(collection_name)
        results = collection.find(query)
        return list(results)
    

    def update_record(self, query, update_values, collection_name=None):
        collection = self.create_collection(collection_name)
        result = collection.update_many(query, {'$set': update_values})
        return result.modified_count
    

    def delete_record(self, query, collection_name=None):
        collection = self.create_collection(collection_name)
        result = collection.delete_many(query)
        return result.deleted_count
