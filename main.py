import requests
import pandas as pd
from datetime import datetime
from forex_python.converter import CurrencyRates, RatesNotAvailableError
import psycopg2
from psycopg2 import sql

def convert_currency(amount, from_currency, to_currency):
    c = CurrencyRates()

    try:
        # Get the exchange rate
        exchange_rate = c.get_rate(from_currency, to_currency)

        # Perform the conversion
        converted_amount = amount * exchange_rate

        return converted_amount
    except RatesNotAvailableError:
        print(f"Error: Currency Rate {from_currency} => {to_currency} not available.")
        return None

def get_top_trending_coins():
    base_url = "https://api.coingecko.com/api/v3"

    # Get current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get market data for the top 10 coins sorted by trading volume
    trending_coins_endpoint = "/coins/markets"
    params_trending_coins = {
        "vs_currency": "usd",
        "order": "volume_desc",
        "per_page": 10,
        "page": 1
    }

    response_trending_coins = requests.get(base_url + trending_coins_endpoint, params=params_trending_coins)
    data_trending_coins = response_trending_coins.json()

    # Create a list of DataFrames for each coin
    dfs = []

    # Convert USD to CAD, EUR, INR, and GBP, and append each coin's DataFrame to the list
    for coin_data in data_trending_coins:
        # Convert USD to CAD, EUR, INR, and GBP
        price_in_usd = coin_data['current_price']
        price_in_cad = convert_currency(price_in_usd, "USD", "CAD")
        price_in_eur = convert_currency(price_in_usd, "USD", "EUR")
        price_in_inr = convert_currency(price_in_usd, "USD", "INR")
        price_in_gbp = convert_currency(price_in_usd, "USD", "GBP")

        # Check if any conversion failed
        if any(amount is None for amount in [price_in_cad, price_in_eur, price_in_inr, price_in_gbp]):
            continue

        coin_df = pd.DataFrame({
            "Timestamp": [timestamp],
            "Name": [coin_data['name']],
            "Symbol": [coin_data['symbol']],
            "Current Price USD": [price_in_usd],
            "Market Cap USD": [coin_data['market_cap']],
            "24h Change": [coin_data['price_change_percentage_24h']],
            "Volume": [coin_data['total_volume']],
            "Current Price CAD": [price_in_cad],
            "Current Price EUR": [price_in_eur],
            "Current Price INR": [price_in_inr],
            "Current Price GBP": [price_in_gbp]
        })
        dfs.append(coin_df)

    # Concatenate the list of DataFrames into a single DataFrame
    df = pd.concat(dfs, ignore_index=True)

    return df

# Get information for the top 10 trending coins and store in a DataFrame
trending_coins_df = get_top_trending_coins()

# Display the DataFrame
print(trending_coins_df)




def move_dataframe_to_postgres(dataframe, table_name, connection_params):
    # Get a PostgreSQL connection
    connection = psycopg2.connect(**connection_params)
    cursor = connection.cursor()

    # Create the table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "Timestamp" TIMESTAMP,
        "Name" VARCHAR(255),
        "Symbol" VARCHAR(10),
        "Current Price USD" NUMERIC,
        "Market Cap USD" NUMERIC,
        "24h Change" NUMERIC,
        "Volume" NUMERIC
    )
    """
    cursor.execute(create_table_query)
    connection.commit()

    # Convert DataFrame to list of tuples and insert into the PostgreSQL table
    records = dataframe.to_records(index=False)
    records_list = list(records)
    insert_query = sql.SQL(f"INSERT INTO {table_name} VALUES %s").format(sql.SQL(',').join(map(sql.Literal, records_list)))
    cursor.execute(insert_query)
    connection.commit()

    # Close the cursor and connection
    cursor.close()
    connection.close()

# Get information for the top 10 trending coins and store in a DataFrame
trending_coins_df = get_top_trending_coins()

# Display the DataFrame
print(trending_coins_df)

# Define PostgreSQL connection parameters
postgres_connection_params = {
    "host": "localhost",
    "port": "5432",
    "user": "your_username",
    "password": "your_password",
    "database": "your_database"
}
table_name = "trending_coins"

# Move the DataFrame to PostgreSQL
move_dataframe_to_postgres(trending_coins_df, table_name, postgres_connection_params)

