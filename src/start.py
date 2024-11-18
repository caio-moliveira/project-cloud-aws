from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from dotenv import load_dotenv
import schedule
import json
import os
import time
import psycopg2
from psycopg2 import sql



load_dotenv()

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'


parameters = {
    'symbol': 'BTC',  # Bitcoin symbol
    'convert': 'USD'  # USD currency
}

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': os.getenv('CMC_API_KEY'),  # from .env
}

# Creating session
session = Session()
session.headers.update(headers)

# RDS enviroment variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Creates a table in case it does not exists
def create_table():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()
        
        # creating table structure
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS bitcoin_quotes (
            id SERIAL PRIMARY KEY,
            price NUMERIC,
            volume_24h NUMERIC,
            market_cap NUMERIC,
            last_updated TIMESTAMP
        );
        '''
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        print("Table created or already exists.")
    except Exception as e:
        print(f"Error trying to create table: {e}")

# Save the data to the database
def save_to_rds(usd_quote):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()
        
        # data ingestion
        insert_query = sql.SQL(
            "INSERT INTO bitcoin_quotes (price, volume_24h, market_cap, last_updated) VALUES (%s, %s, %s, %s)"
        )
        cursor.execute(insert_query, (
            usd_quote['price'],
            usd_quote['volume_24h'],
            usd_quote['market_cap'],
            usd_quote['last_updated']
        ))
        conn.commit()
        cursor.close()
        conn.close()
        print("Data saved successfully!")
    except Exception as e:
        print(f"Error saving data to RDS: {e}")

def consult_bitcoin_quote():
    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        
        # Verify if data are present on response
        if 'data' in data and 'BTC' in data['data']:
            bitcoin_data = data['data']['BTC']
            usd_quote = bitcoin_data['quote']['USD']
            
            # Saving 
            save_to_rds(usd_quote)
        else:
            print("Error getting Bitcoin quote:", data['status'].get('error_message', 'Error unkown'))

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(f"Error on request: {e}")


# Call the function to create the table
create_table()

# Schedule to run every 15s
schedule.every(5).seconds.do(consult_bitcoin_quote)

# Loop on main to schedule 
if __name__ == "__main__":
    print("Initializing in 15s...")
    while True:
        schedule.run_pending()
        time.sleep(1)