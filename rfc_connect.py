from pyrfc import Connection


def fetch_sales_orders_rfc(host, sysnr, client, user, password):
    try:
        # Establishing RFC connection
        connection = Connection(
            user=user,
            passwd=password,
            ashost=host,  # SAP Application Server
            sysnr=sysnr,  # SAP System Number
            client=client  # SAP Client Number
        )
        print("RFC connection established.")

        # Calling the BAPI function module to fetch sales orders
        result = connection.call("BAPI_SALESORDER_GETLIST",
                                 CUSTOMER_NUMBER="123456",  # Example customer number
                                 SALES_ORGANIZATION="1000",  # Example sales org
                                 DISTRIBUTION_CHANNEL="01",  # Example channel
                                 DIVISION="01")  # Example division

        # Extracting sales order data from the result
        sales_orders = result.get("SALES_ORDERS", [])
        print(f"Fetched {len(sales_orders)} sales orders.")

        # Close the RFC connection
        connection.close()
        print("RFC connection closed.")

        return sales_orders

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


# Example Usage
if __name__ == "__main__":
    host = "sap_hostname"  # SAP system hostname or IP
    sysnr = "00"  # SAP system number (e.g., 00, 01, etc.)
    client = "100"  # SAP client number
    user = "your_username"  # SAP username
    password = "your_password"  # SAP password

    sales_orders = fetch_sales_orders_rfc(host, sysnr, client, user, password)

    if sales_orders:
        for order in sales_orders:
            print(order)
