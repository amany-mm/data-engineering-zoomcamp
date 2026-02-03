import pandas as pd
from sqlalchemy import create_engine
from time import time


def ingest_data(parquet_file, csv_file, table_name_trips, table_name_zones):
    """
    Ingest NYC taxi data into PostgreSQL
    """

    engine = create_engine(
        'postgresql://postgres:postgres@localhost:5433/ny_taxi')

    print("Connected to PostgreSQL database")

    # Load and ingest the parquet file (green taxi trips)
    print(f"\nLoading {parquet_file}...")
    df_trips = pd.read_parquet(parquet_file)

    print(f"Loaded {len(df_trips)} rows")
    print(f"Columns: {df_trips.columns.tolist()}")

    print("\nData types:")
    print(df_trips.dtypes)

    # Insert data in chunks
    print(f"\nInserting data into {table_name_trips} table...")
    t_start = time()

    df_trips.to_sql(name=table_name_trips, con=engine,
                    if_exists='replace', index=False, chunksize=10000)

    t_end = time()
    print(f"Inserted {len(df_trips)} rows in {t_end - t_start:.2f} seconds")

    # Load and ingest the CSV file (zone lookup)
    print(f"\nLoading {csv_file}...")
    df_zones = pd.read_csv(csv_file)

    print(f"Loaded {len(df_zones)} rows")
    print(f"Columns: {df_zones.columns.tolist()}")

    print(f"\nInserting data into {table_name_zones} table...")
    df_zones.to_sql(name=table_name_zones, con=engine,
                    if_exists='replace', index=False)

    print(f"Inserted {len(df_zones)} rows")

    print("\n Data ingestion completed successfully!")

    # Show sample data
    print("\nSample trip data:")
    print(df_trips.head(3))

    print("\nSample zone data:")
    print(df_zones.head(3))


if __name__ == "__main__":

    parquet_file = "green_tripdata_2025-11.parquet"
    csv_file = "taxi_zone_lookup.csv"

    table_name_trips = "green_taxi_trips"
    table_name_zones = "taxi_zone_lookup"

    # Run ingestion
    ingest_data(parquet_file, csv_file, table_name_trips, table_name_zones)
