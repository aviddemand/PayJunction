import yaml
import json
import os
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def load_list_from_file(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file if line.strip()]

ACCESS_TOKEN = 'ya29.a0AcM612wxJBViTQc88lTecoDg32d9AV8AgeYkOgsuU7Uc_mopCQfntiZ-ZXLUnEFlHs4BXKDZpgKDXNLjGtcnN2dGzunRK1VmKNRAe_IX7mHZaSsgx7nWKs44bukU5LpYV3pQI_iykTs18wgxOQMiAEIFldj1AhnfAuhXBUqUaCgYKAV0SARESFQHGX2Mi9bkE76MHqPoN1ZfNOtG_BQ0175'

# Create an instance of Credentials with the access token
credentials = Credentials(token=ACCESS_TOKEN)

service = build('searchconsole', 'v1', credentials=credentials)

# Read date ranges from file
dates = load_list_from_file("dates.txt")

# Database connection
engine = create_engine('mysql+pymysql://mainlogan:Mrmojo69@35.222.252.227/payjunction')

try:
    for date in dates:
        print(f"Processing data for date: {date}")

        # Define the request body for Google Search Console
        request = {
            'startDate': date,
            'endDate': date,
            'dimensions': ['date'],
            'rowLimit': 10000,  # Adjust as necessary
        }

        # Execute the request
        response = service.searchanalytics().query(siteUrl='https://www.payjunction.com', body=request).execute()

        # Extract data
        data = []
        if 'rows' in response:
            for row in response['rows']:
                date = row['keys'][0]
                impressions = row['impressions']
                data.append([date, impressions])

        # Convert data to DataFrame
        df = pd.DataFrame(data, columns=["Date", "Impressions"])
        df["Month"] = pd.to_datetime(df["Date"]).dt.strftime('%Y-%m-%d')  # Format as YYYY-MM-DD
        df = df.drop(columns=["Date"])  # Remove the 'Date' column, keeping 'Month', 'Page', and 'Impressions'

        # Print the data to be inserted for debugging
        print(f"Data to be inserted:\n{df}")

        # Insert data into MySQL
        with engine.begin() as connection:
            df.to_sql('overall_gsc_data', con=connection, if_exists='append', index=False)

except SQLAlchemyError as db_ex:
    print(f"An error occurred with the database: {db_ex}")

finally:
    engine.dispose()
