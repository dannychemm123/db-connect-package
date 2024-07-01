# File: tests/unit/test_mongo_crud.py

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

import pytest
from dan_db_connect.mongo_crud import MongoDBOperation
from pymongo.errors import ServerSelectionTimeoutError

@pytest.fixture
def mongodb_client():
    client_uri = "mongodb+srv://dannychemm121:5LvTUg6Zgpn9sV4u@cluster0.nafe4q8.mongodb.net/?appName=Cluster0"
    database_name = "contidan"
    collection_name = "students"
    return MongoDBOperation(client_uri, database_name, collection_name)

def test_create_client(mongodb_client):
    client = mongodb_client.create_client()
    assert client is not None

def test_insert_record(mongodb_client):
    record = {"name": "John", "age": 30, "sex": "male", "class": "level 300"}
    try:
        mongodb_client.delete({}, "students")  # Clear collection
        mongodb_client.insert_record(record, "students")
        result = list(mongodb_client.find({"name": "John"}, "students"))
        assert len(result) == 1
    except ServerSelectionTimeoutError:
        pytest.skip("MongoDB server is not available")

def test_bulk_insert(mongodb_client, tmpdir):
    datafile = tmpdir.join("test_data.csv")
    datafile.write("name,age,sex,class\nJohn,30,male,level 300\nJane,25,female,level 400")
    try:
        mongodb_client.delete({}, "students")  # Clear collection
        mongodb_client.bulk_insert(str(datafile), "students")
        result = list(mongodb_client.find({}, "students"))
        assert len(result) >= 2
    except ServerSelectionTimeoutError:
        pytest.skip("MongoDB server is not available")

def test_find(mongodb_client):
    try:
        mongodb_client.delete({}, "students")  # Clear collection
        mongodb_client.insert_record({"name": "John", "age": 30, "sex": "male", "class": "level 300"}, "students")
        result = list(mongodb_client.find({"name": "John"}, "students"))
        assert len(result) == 1
    except ServerSelectionTimeoutError:
        pytest.skip("MongoDB server is not available")

def test_delete(mongodb_client):
    # Clear collection
    mongodb_client.delete({}, "students")
    
    # Insert a record
    mongodb_client.insert_record({"name": "daniel", "age": 30, "sex": "male", "class": "level 300"}, "students")
    
    # Verify insertion
    result = list(mongodb_client.find({"name": "daniel"}, "students"))
    print("Before delete, records found: ", result)
    
    try:
        # Perform deletion
        mongodb_client.delete({"name": "daniel"}, "students")
        
        # Verify deletion
        result = list(mongodb_client.find({"name": "daniel"}, "students"))
        print("After delete, records found: ", result)
        
        assert len(result) == 0
    except ServerSelectionTimeoutError:
        pytest.skip("MongoDB server is not available")

def test_update(mongodb_client):
    record = {"name": "Jane", "age": 25, "sex": "female", "class": "level 400"}
    try:
        mongodb_client.delete({}, "students")  # Clear collection
        mongodb_client.insert_record(record, "students")
        mongodb_client.update({"name": "Jane"}, {"age": 26}, "students")
        result = list(mongodb_client.find({"name": "Jane"}, "students"))
        assert result[0]["age"] == 26
    except ServerSelectionTimeoutError:
        pytest.skip("MongoDB server is not available")
