from db_utils.PostgreSQL import PostgreSQLConnector
from db_utils.MySQL import MySQLConnector



# Initialize the connection
postgre_db = PostgreSQLConnector(host='localhost', user='postgres', password='password', database='test_db')
mysql_db = MySQLConnector(host='localhost', user='root', password='password', database='test_db')

# Connect to the database
postgre_db.connect()
mysql_db.connect()

# Execute a query
postgre_db.execute_query("SELECT LEFT(VISIT_DATE,7) as month, COUNT(*) AS NUM_OF_PATIENT FROM PATIENT GROUP BY month order by 1")
postgre_db.execute_query("SELECT LEFT(VISIT_DATE,7) as month, COUNT(*) AS NUM_OF_PATIENT FROM PATIENT GROUP BY month order by 1")

# Insert data
postgre_db.insert_data('users', {'name': 'Jane Doe', 'email': 'jane.doe@example.com'})

# Fetch data
results = postgre_db.fetch_results("SELECT * FROM users")
for row in results:
    print(row)

# Update data
postgre_db.update_data('users', {'email': 'jane.doe@newdomain.com'}, "name = 'Jane Doe'")

# Delete data
postgre_db.delete_data('users', "name = 'Jane Doe'")

# Disconnect from the database
postgre_db.disconnect()
