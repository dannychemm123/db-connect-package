# File: src/dan_db_connect/mongo_crud.py

from typing import Optional
import pandas as pd
import json
from pymongo import MongoClient


class MongoDBOperation:
    """
    MongoDB operations for creating client, database, collections and performing CRUD operations.
    """

    def __init__(
        self, client_uri: str, database_name: str, collection_name: Optional[str] = None
    ):
        self.client_uri = client_uri
        self.database_name = database_name
        self.collection_name = collection_name

    def create_client(self) -> MongoClient:
        """
        Creates a MongoDB client.
        """
        client = MongoClient(self.client_uri)
        return client

    def create_database(self):
        """
        Creates and returns the MongoDB database.
        """
        client = self.create_client()
        database = client[self.database_name]
        return database

    def create_collection(self, collection_name: str):
        """
        Creates and returns the MongoDB collection.
        """
        database = self.create_database()
        collection = database[collection_name]
        return collection

    def insert_record(self, record: dict, collection_name: str):
        """
        Inserts a single record into the specified collection.
        """
        if not isinstance(record, dict):
            raise TypeError("record must be a dictionary")
        collection = self.create_collection(collection_name)
        collection.insert_one(record)

    def bulk_insert(self, datafile: str, collection_name: Optional[str] = None):
        """
        Bulk inserts records from a file (CSV or Excel) into the specified collection.
        """
        if datafile.endswith(".csv"):
            data = pd.read_csv(datafile, encoding="utf-8")
        elif datafile.endswith(".xlsx"):
            data = pd.read_excel(datafile, encoding="utf-8")
        else:
            raise ValueError("Unsupported file format. Use CSV or Excel files.")
        data_json = json.loads(data.to_json(orient="records"))
        collection = self.create_collection(collection_name)
        collection.insert_many(data_json)

    def find(self, query: dict, collection_name: Optional[str] = None):
        """
        Finds and returns records matching the query from the specified collection.
        """
        collection = self.create_collection(collection_name)
        return list(collection.find(query))
    
   


    def delete(self, query: dict, collection_name: Optional[str] = None):
        """
        Deletes records matching the query from the specified collection.
        """
        collection = self.create_collection(collection_name)
        collection.delete_one(query)

    def update(
        self, query: dict, update_values: dict, collection_name: Optional[str] = None
    ):
        """
        Updates records matching the query with the specified update values in the specified collection.
        """
        collection = self.create_collection(collection_name)
        collection.update_one(query, {"$set": update_values})
