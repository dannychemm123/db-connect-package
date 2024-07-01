from typing import Optional, List, Dict, Any
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
        client: MongoClient = MongoClient(self.client_uri)
        return client

    def create_database(self) -> Any:
        """
        Creates and returns the MongoDB database.
        """
        client = self.create_client()
        database = client[self.database_name]
        return database

    def create_collection(self, collection_name: str) -> Any:
        """
        Creates and returns the MongoDB collection.
        """
        database = self.create_database()
        collection = database[collection_name]
        return collection

    def insert_record(self, record: Dict[str, Any], collection_name: str) -> None:
        """
        Inserts a single record into the specified collection.
        """
        if not isinstance(record, dict):
            raise TypeError("record must be a dictionary")
        collection = self.create_collection(collection_name)
        collection.insert_one(record)

    def bulk_insert(self, datafile: str, collection_name: Optional[str] = None) -> None:
        """
        Bulk inserts records from a file (CSV or Excel) into the specified collection.
        """
        if collection_name is None:
            if self.collection_name is None:
                raise ValueError("Collection name must be provided")
            collection_name = self.collection_name
        if datafile.endswith(".csv"):
            data = pd.read_csv(datafile, encoding="utf-8")
        elif datafile.endswith(".xlsx"):
            data = pd.read_excel(datafile, encoding="utf-8")
        else:
            raise ValueError("Unsupported file format. Use CSV or Excel files.")
        data_json = json.loads(data.to_json(orient="records"))
        collection = self.create_collection(collection_name)
        collection.insert_many(data_json)

    def find(self, query: Dict[str, Any], collection_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Finds and returns records matching the query from the specified collection.
        """
        if collection_name is None:
            if self.collection_name is None:
                raise ValueError("Collection name must be provided")
            collection_name = self.collection_name

        collection = self.create_collection(collection_name)
        return list(collection.find(query))

    def delete(self, query: Dict[str, Any], collection_name: Optional[str] = None) -> None:
        """
        Deletes records matching the query from the specified collection.
        """
        if collection_name is None:
            if self.collection_name is None:
                raise ValueError("Collection name must be provided")
            collection_name = self.collection_name

        collection = self.create_collection(collection_name)
        collection.delete_one(query)

    def update(
        self, query: Dict[str, Any], update_values: Dict[str, Any], collection_name: Optional[str] = None
    ) -> None:
        """
        Updates records matching the query with the specified update values in the specified collection.
        """
        if collection_name is None:
            if self.collection_name is None:
                raise ValueError("Collection name must be provided")
            collection_name = self.collection_name

        collection = self.create_collection(collection_name)
        collection.update_one(query, {"$set": update_values})
