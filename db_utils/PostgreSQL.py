import psycopg2
from psycopg2 import OperationalError

class PostgreSQLConnector:
    def __init__(self, host, user, password, database, port=5432):
        """Initialize the connection to the PostgreSQL server."""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            print(f"Connected to PostgreSQL database '{self.database}'")
        except OperationalError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            self.connection = None

    def disconnect(self):
        """Close the connection to the PostgreSQL database."""
        if self.connection:
            self.connection.close()
            print("PostgreSQL connection closed.")

    def execute_query(self, query, params=None):
        """Execute a single SQL query."""
        if self.connection:
            cursor = self.connection.cursor()
            try:
                cursor.execute(query, params)
                self.connection.commit()
                print("Query executed successfully.")
            except OperationalError as e:
                print(f"Error executing query: {e}")
            finally:
                cursor.close()
        else:
            print("No active PostgreSQL connection.")

    def fetch_results(self, query, params=None):
        """Execute a SELECT query and fetch the results."""
        results = None
        if self.connection:
            cursor = self.connection.cursor()
            try:
                cursor.execute(query, params)
                results = cursor.fetchall()
            except OperationalError as e:
                print(f"Error fetching data: {e}")
            finally:
                cursor.close()
        else:
            print("No active PostgreSQL connection.")
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
