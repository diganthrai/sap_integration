from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
from pyrfc import Connection
from dotenv import load_dotenv
import os
from datetime import datetime

# Step 1: Load environment variables
load_dotenv()
sap_host = os.getenv("SAP_ASHOST")
sap_sysnr = os.getenv("SAP_SYSNR")
sap_client = os.getenv("SAP_CLIENT")
sap_user = os.getenv("SAP_USER")
sap_passwd = os.getenv("SAP_PASSWD")
adls_account_url = os.getenv("ADLS_ACCOUNT_URL")
adls_container_name = os.getenv("ADLS_CONTAINER_NAME")
adls_sas_token = os.getenv("ADLS_SAS_TOKEN")

# Step 2: Initialize Spark session with ADLS configuration
spark = SparkSession.builder \
    .appName("SAP Delta Capture to ADLS") \
    .config("spark.hadoop.fs.azure.account.auth.type", "SAS") \
    .config(f"fs.azure.sas.{adls_container_name}.{adls_account_url}", adls_sas_token) \
    .config("spark.jars", "/path/to/hadoop-azure-jars/*.jar") \
    .getOrCreate()

print("Spark session initialized.")

# Step 3: Connect to SAP
try:
    conn = Connection(ashost=sap_host, sysnr=sap_sysnr, client=sap_client, user=sap_user, passwd=sap_passwd)
    print("Connected to SAP.")
except Exception as e:
    print(f"Error connecting to SAP: {e}")
    exit()

# Step 4: Fetch last extraction timestamp
try:
    with open('last_extraction_time.txt', 'r') as f:
        last_extraction = f.read().strip()
except FileNotFoundError:
    last_extraction = "20230101000000"  # Default to Jan 1, 2023
last_extraction_date = last_extraction[:8]  # YYYYMMDD
last_extraction_time = last_extraction[8:]  # HHMMSS

# Step 5: Fetch delta records
delta_query = f"""
    SELECT ID, DATA, LAST_CHANGED_DATE, LAST_CHANGED_TIME
    FROM ZMY_TABLE
    WHERE LAST_CHANGED_DATE > '{last_extraction_date}'
      OR (LAST_CHANGED_DATE = '{last_extraction_date}' AND LAST_CHANGED_TIME > '{last_extraction_time}')
"""
try:
    delta_result = conn.call('RFC_READ_TABLE', QUERY_TABLE='ZMY_TABLE', OPTIONS=[{'TEXT': delta_query}])
    delta_schema = StructType([
        StructField("ID", StringType(), True),
        StructField("DATA", StringType(), True),
        StructField("LAST_CHANGED_DATE", DateType(), True),
        StructField("LAST_CHANGED_TIME", TimestampType(), True)
    ])
    delta_df = spark.createDataFrame(delta_result['DATA'], schema=delta_schema)
    print("Delta records fetched successfully.")
except Exception as e:
    print(f"Error fetching delta records: {e}")
    delta_df = spark.createDataFrame([], StructType([]))  # Empty DataFrame

# Step 6: Write delta records to ADLS
output_path = f"abfss://{adls_container_name}@{adls_account_url}/delta_records/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
delta_df.write.mode("overwrite").parquet(output_path)
print(f"Delta records written to {output_path}")

# Step 7: Update last extraction timestamp
new_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
with open('last_extraction_time.txt', 'w') as f:
    f.write(new_timestamp)
print(f"Last extraction timestamp updated to {new_timestamp}")

# Close SAP connection
conn.close()
print("SAP connection closed.")
