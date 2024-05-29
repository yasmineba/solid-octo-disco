import trino
import time
import os
import urllib3
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

os.environ['http_proxy'] = "http://proxyparfil.si.fr.intraorange:8080"
os.environ['https_proxy'] = "http://proxyparfil.si.fr.intraorange:8080"




def connect_to_starburst(user, password):
    conn = trino.dbapi.connect(
        host='stargate-poc.com.intraorange',  # Replace with your Starburst host
        port=443,  # Replace with your Starburst port
        user=user,
        http_scheme='https',  # or 'https' if your server uses SSL
        catalog='stargate-kpi',
        auth=trino.auth.BasicAuthentication(user, password),
        verify=False
    )
    return conn

def watch_table(conn, schema, table):
    cur = conn.cursor()
    last_check_time = None

    while True:
        query = f"""
        SELECT *
        FROM {schema}.{table}
        """
        if last_check_time:
            query += f" WHERE query_end_time > TIMESTAMP '{last_check_time}'"
        
        cur.execute(query)
        rows = cur.fetchmany(3)  # Fetch three sets of rows
        
        for row_set in rows:
            for row in row_set:
                print(row)
        
        if rows:
            last_check_time = rows[-1][3]  # Update last_check_time if rows were fetched
        
        time.sleep(10)  # Polling interval in seconds
    
def main():
    user = 'yasmine'
    password = 'yasmine123'
    

    conn = connect_to_starburst(user, password)
    watch_table(conn, 'public', 'completed_queries')

if __name__ == "__main__":
    main()
