import requests
from requests.auth import HTTPBasicAuth

# SAP API endpoint
sap_url = "https://<sap-system-url>/sap/opu/odata/sap/<service_name>/EntitySet"

# Basic authentication credentials
username = "your_username"
password = "your_password"

# Send GET request
response = requests.get(sap_url, auth=HTTPBasicAuth(username, password))

# Check the response status
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    print("Data fetched successfully!")
    print(data)
else:
    print(f"Error: {response.status_code} - {response.text}")
