import mysql.connector
from mysql.connector import Error

class MySQLConnector:
    def __init__(self, host, user, password, database):
        """Initialize the connection to the MySQL server."""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        """Establish a connection to the MySQL database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                print(f"Connected to MySQL database '{self.database}'")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            self.connection = None

    def disconnect(self):
        """Close the connection to the MySQL database."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection closed.")

    def execute_query(self, query, params=None):
        """Execute a single SQL query."""
        if self.connection and self.connection.is_connected():
            cursor = self.connection.cursor()
            try:
                cursor.execute(query, params)
                self.connection.commit()
                print("Query executed successfully.")
            except Error as e:
                print(f"Error executing query: {e}")
            finally:
                cursor.close()
        else:
            print("No active MySQL connection.")

    def fetch_results(self, query, params=None):
        """Execute a SELECT query and fetch the results."""
        results = None
        if self.connection and self.connection.is_connected():
            cursor = self.connection.cursor()
            try:
                cursor.execute(query, params)
                results = cursor.fetchall()
            except Error as e:
                print(f"Error fetching data: {e}")
            finally:
                cursor.close()
        else:
            print("No active MySQL connection.")
        return results

    def insert_data(self, table, data):
        """Insert data into a specific table."""
        placeholders = ', '.join(['%s'] * len(data))
        columns = ', '.join(data.keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.execute_query(query, tuple(data.values()))

    def update_data(self, table, data, condition):
        """Update data in a specific table."""
        updates = ', '.join([f"{k} = %s" for k in data.keys()])
        query = f"UPDATE {table} SET {updates} WHERE {condition}"
        self.execute_query(query, tuple(data.values()))

    def delete_data(self, table, condition):
        """Delete data from a specific table."""
        query = f"DELETE FROM {table} WHERE {condition}"
        self.execute_query(query)
