import mysql.connector
from mysql.connector import Error


class MySQLOperation:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
    def create_connection(self):
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print("MySQL Database connection successful")
        except Error as e:
            print(f"The error '{e}' occurred")
        return connection
    def execute_query(self, query, values=None):
        connection = self.create_connection()
        cursor = connection.cursor()
        try:
            if values:
                cursor.execute(query, values)
            else:
                cursor.execute(query)
            connection.commit()
            print("Query executed successfully")
        except Error as e:
            print(f"The error '{e}' occurred")
    def execute_read_query(self, query):
        connection = self.create_connection()
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"The error '{e}' occurred")
    def insert_record(self, table, record):
        keys = ", ".join(record.keys())
        values = tuple(record.values())
        placeholders = ", ".join(["%s"] * len(record))
        query = f"INSERT INTO {table} ({keys}) VALUES ({placeholders})"
        self.execute_query(query, values)    
    def bulk_insert(self, table, datafile):
        import pandas as pd
        data = None
        if datafile.endswith(".csv"):
            data = pd.read_csv(datafile)
        elif datafile.endswith(".xlsx"):
            data = pd.read_excel(datafile)

        if data is not None:
            for _, row in data.iterrows():
                self.insert_record(table, row.to_dict())
    def find_records(self, table, condition=None):
        query = f"SELECT * FROM {table}"
        if condition:
            query += f" WHERE {condition}"
        return self.execute_read_query(query)
    def update_record(self, table, update_values, condition):
        set_clause = ", ".join([f"{key} = %s" for key in update_values.keys()])
        values = tuple(update_values.values())
        query = f"UPDATE {table} SET {set_clause} WHERE {condition}"
        self.execute_query(query, values)
    def delete_record(self, table, condition):
        query = f"DELETE FROM {table} WHERE {condition}"
        self.execute_query(query)

