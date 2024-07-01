# File: tests/unit/test_sql_crud.py

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

import pytest
from dan_db_connect.sql_crud import MySQLDBOperation
from mysql.connector.errors import InterfaceError

@pytest.fixture
def mysql_client():
    return MySQLDBOperation(host="localhost", user="root", password="contidan", database="contidan")

def test_create_connection(mysql_client):
    connection = mysql_client.create_connection()
    assert connection is not None

def test_insert_record(mysql_client):
    try:
        mysql_client.execute_query("CREATE TABLE IF NOT EXISTS students (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), age INT, sex VARCHAR(10), class VARCHAR(20))")
        record = {"name": "Joe", "age": 30, "sex": "male", "class": "level 300"}
        mysql_client.insert_record("students", record)
        result = mysql_client.find("SELECT * FROM students WHERE name = 'Joe'")
        assert result is not None
        assert len(result) == 1
    except InterfaceError:
        pytest.skip("MySQL server is not available")

def test_bulk_insert(mysql_client, tmpdir):
    datafile = tmpdir.join("test_data.csv")
    datafile.write("name,age,sex,class\ndaniel,30,male,level 300\nJanet,25,female,level 400")
    try:
        mysql_client.bulk_insert("students", str(datafile))
        result = mysql_client.find("SELECT * FROM students")
        assert result is not None
        assert len(result) >= 2
    except InterfaceError:
        pytest.skip("MySQL server is not available")

def test_find(mysql_client):
    try:
        result = mysql_client.find("SELECT * FROM students WHERE name = 'daniel'")
        assert result is not None
        assert len(result) == 1
    except InterfaceError:
        pytest.skip("MySQL server is not available")

def test_delete(mysql_client):
    try:
        mysql_client.delete("students", "name = 'John'")
        result = mysql_client.find("SELECT * FROM students WHERE name = 'John'")
        assert result == []
    except InterfaceError:
        pytest.skip("MySQL server is not available")

def test_update(mysql_client):
    record = {"name": "Jane", "age": 25, "sex": "female", "class": "level 400"}
    try:
        mysql_client.insert_record("students", record)
        mysql_client.update("students", {"age": 26}, "name = 'Jane'")
        result = mysql_client.find("SELECT * FROM students WHERE name = 'Jane'")
        assert result is not None
        assert result[0]["age"] == 26
    except InterfaceError:
        pytest.skip("MySQL server is not available")


