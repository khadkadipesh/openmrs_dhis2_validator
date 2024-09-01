import mysql.connector
from mysql.connector import Error
from sshtunnel import SSHTunnelForwarder

class MySQLConnectorSSH:
    def __init__(self, ssh_host, ssh_username, ssh_password, remote_bind_address, mysql_user, mysql_password, mysql_database, ssh_port=22, local_bind_port=3306):
        """Initialize the SSH tunnel and MySQL connection parameters."""
        self.ssh_host = ssh_host
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_port = ssh_port
        self.remote_bind_address = remote_bind_address
        self.local_bind_port = local_bind_port
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_database = mysql_database
        self.server = None
        self.connection = None

    def start_ssh_tunnel(self):
        """Start the SSH tunnel."""
        self.server = SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_username,
            ssh_password=self.ssh_password,
            remote_bind_address=self.remote_bind_address,
            local_bind_address=('0.0.0.0', self.local_bind_port)
        )
        self.server.start()
        print(f"SSH tunnel established on localhost:{self.local_bind_port}")

    def connect(self):
        """Establish a connection to the MySQL database through the SSH tunnel."""
        self.start_ssh_tunnel()
        try:
            self.connection = mysql.connector.connect(
                host='127.0.0.1',
                user=self.mysql_user,
                password=self.mysql_password,
                database=self.mysql_database,
                port=self.local_bind_port
            )
            if self.connection.is_connected():
                print(f"Connected to MySQL database '{self.mysql_database}' through SSH tunnel")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            self.connection = None

    def disconnect(self):
        """Close the MySQL connection and stop the SSH tunnel."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection closed.")
        if self.server:
            self.server.stop()
            print("SSH tunnel closed.")

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
