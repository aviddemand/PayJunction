import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from google.oauth2.credentials import Credentials

# OAuth 2.0 Access Token
ACCESS_TOKEN = 'ya29.a0AcM612zlmxKmaXiHFU1d9c673G5P3mgb8ckMtRFPQ1d2fDqcLPvIRyvk2aokUmHvFT0zkLW_qq2Z5JVuz1WOrK7i-Faum8EyvonxTv1zJESWErCzZn41V3hJezRzzFht6h1lrCUdelTGcePxaZudvQKsJvlyEHXUs2Pk4dXraCgYKAdsSARESFQHGX2MiMtbjmkRYyfqVLrqVOTw6dw0175'
DEVELOPER_TOKEN = 'HeWAMQmV1_lnhNk2UTS40g'
LOGIN_CUSTOMER_ID = '8970659849'

# Create an instance of Credentials with the access token
credentials = Credentials(token=ACCESS_TOKEN)

# Initialize Google Ads Client without YAML
client = GoogleAdsClient(
    credentials=credentials,
    developer_token=DEVELOPER_TOKEN,
    login_customer_id=LOGIN_CUSTOMER_ID
)

# Set the specific client account ID you want to query
client_id = "9539576235"  # Replace with the actual client account ID

# Database connection
engine = create_engine('mysql+pymysql://mainlogan:Mrmojo69@35.222.252.227/payjunction')

# Read date ranges from file, assuming each line in dates.txt is a single date 'YYYY-MM-DD'
date_ranges = []
with open("dates.txt", "r") as file:
    for line in file:
        date = line.strip()
        if date:
            date_ranges.append((date, date))  # Use the same date as both start and end

print("Date ranges to process:", date_ranges)

data = []

for start_date, end_date in date_ranges:
    print(f"Processing data for client {client_id} from {start_date} to {end_date}")

    # Define query to fetch impressions data
    query = f"""
        SELECT
            segments.date,
            metrics.impressions
        FROM
            customer
        WHERE
            segments.date BETWEEN '{start_date}' AND '{end_date}'
    """

    try:
        ga_service = client.get_service("GoogleAdsService")
        response = ga_service.search_stream(customer_id=client_id, query=query)

        for batch in response:
            for row in batch.results:
                date = row.segments.date
                impressions = row.metrics.impressions
                data.append([date, impressions])

    except GoogleAdsException as ex:
        print(f"An error occurred with Google Ads API for client {client_id}: {ex.error}")

# Convert to DataFrame
df = pd.DataFrame(data, columns=["Date", "Impressions"])
df["Date"] = pd.to_datetime(df["Date"], format='%Y-%m-%d')
df.rename(columns={"Date": "Month"}, inplace=True)

# Insert data into MySQL
try:
    with engine.begin() as connection:
        df.to_sql('overall_gads_data', con=connection, if_exists='append', index=False)

    print('Data successfully inserted into MySQL.')

except SQLAlchemyError as db_ex:
    print(f"An error occurred with the database: {db_ex}")
finally:
    engine.dispose()
