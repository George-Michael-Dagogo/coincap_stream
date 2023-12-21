import requests
import pandas as pd
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2 import sql
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

    # Create a DataFrame to store the information
    columns = ["Timestamp", "Name", "Symbol", "Current Price USD", "Market Cap USD", "24h Change", "Volume"]
    df = pd.DataFrame(columns=columns)

    # Concatenate the DataFrame with a new DataFrame for each coin
    for coin_data in data_trending_coins:
        coin_df = pd.DataFrame({
            "Timestamp": [timestamp],
            "Name": [coin_data['name']],
            "Symbol": [coin_data['symbol']],
            "Current Price USD": [coin_data['current_price']],
            "Market Cap USD": [coin_data['market_cap']],
            "24h Change": [coin_data['price_change_percentage_24h']],
            "Volume": [coin_data['total_volume']]
        })
        df = pd.concat([df, coin_df], ignore_index=True)

    return df

# Get information for the top 10 trending coins and store in a DataFrame
trending_coins_df = get_top_trending_coins()

# Display the DataFrame



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

