import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

# Create a database connection
engine = create_engine('postgresql+psycopg2://postgres:secret@localhost/tpch')
conn = engine.connect()

# Read the Parquet files
folder_path = '/Users/bvolpato/Downloads/tpch/'
for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)

    if not os.path.isfile:
        continue

    df = pd.read_parquet(file_path)

    table_name = filename.split('.')[0]

    print(f'Importing {len(df)} rows into "{table_name}"...')
    df.to_sql(table_name, conn, if_exists='replace', index=False)

# Close the connection
conn.close()
