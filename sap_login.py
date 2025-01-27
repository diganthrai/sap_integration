from hdbcli import dbapi

# Establish connection to the SAP HANA database
conn = dbapi.connect(
    address='your_hana_host',
    port=30015,  # default port
    user='your_username',
    password='your_password'
)

# Create a cursor to interact with the database
cursor = conn.cursor()

# Execute a query
cursor.execute("SELECT CURRENT_DATE FROM DUMMY")

# Fetch the result
result = cursor.fetchone()
print(result)

# Close the connection
cursor.close()
conn.close()




#test setup 01