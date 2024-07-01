# File: src/dan_db_connect/sql_crud.py

import mysql.connector
from mysql.connector import Error
from typing import Optional, Dict, Any
import pandas as pd


class MySQLDBOperation:
    """
    MySQL operations for creating connection and performing CRUD operations.
    """

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def create_connection(self):
        """
        Creates and returns the MySQL database connection.
        """
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            if connection.is_connected():
                print("Connection to MySQL DB successful")
        except Error as e:
            print(f"The error '{e}' occurred")
        return connection

    def execute_query(self, query: str):
        """
        Executes a single query in the database.
        """
        connection = self.create_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            connection.commit()
            print("Query executed successfully")
        except Error as e:
            print(f"The error '{e}' occurred")

    def insert_record(self, table: str, record: Dict[str, Any]):
        """
        Inserts a single record into the specified table.
        """
        connection = self.create_connection()
        cursor = connection.cursor()
        columns = ", ".join(record.keys())
        values = ", ".join(["%s"] * len(record))
        sql = f"INSERT INTO {table} ({columns}) VALUES ({values})"
        try:
            cursor.execute(sql, tuple(record.values()))
            connection.commit()
            print("Record inserted successfully")
        except Error as e:
            print(f"The error '{e}' occurred")

    def bulk_insert(self, table: str, datafile: str):
        """
        Bulk inserts records from a file (CSV or Excel) into the specified table.
        """
        if datafile.endswith(".csv"):
            data = pd.read_csv(datafile, encoding="utf-8")
        elif datafile.endswith(".xlsx"):
            data = pd.read_excel(datafile, encoding="utf-8")
        else:
            raise ValueError("Unsupported file format. Use CSV or Excel files.")
        records = data.to_dict(orient="records")
        for record in records:
            self.insert_record(table, record)

    def find(self, query: str):
        """
        Finds and returns records matching the query from the specified table.
        """
        connection = self.create_connection()
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        return result

    def delete(self, table: str, condition: str):
        """
        Deletes records matching the condition from the specified table.
        """
        query = f"DELETE FROM {table} WHERE {condition}"
        self.execute_query(query)

    def update(self, table: str, updates: Dict[str, Any], condition: str):
        """
        Updates records matching the condition with the specified updates in the specified table.
        """
        set_clause = ", ".join([f"{key} = %s" for key in updates.keys()])
        sql = f"UPDATE {table} SET {set_clause} WHERE {condition}"
        connection = self.create_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(sql, tuple(updates.values()))
            connection.commit()
            print("Record updated successfully")
        except Error as e:
            print(f"The error '{e}' occurred")
