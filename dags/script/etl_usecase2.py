import pandas as pd
from pandasql import sqldf
from sqlalchemy import create_engine

# Konfigurasi database
db_uri = 'postgresql+psycopg2://postgres:12345@host.docker.internal:5432/de_mentor'
#f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

# Buat engine SQLAlchemy
engine = create_engine(db_uri)

# Extract Data Needed from parquet file and csv files
df_parquet = pd.read_parquet('/opt/airflow/dags/dataset/green_tripdata_2019-01.parquet')
df_taxi_zonelookup = pd.read_csv('/opt/airflow/dags/dataset/taxi+_zone_lookup.csv')

# Do Data Transformation (Join 2 table become 1 table)
df_parquet_column = sqldf(''' SELECT VendorID,
                          lpep_pickup_datetime,
                          lpep_dropoff_datetime,
                          RatecodeID,
                          PULocationID,
                          DOLocationID,
                          passenger_count,
                          trip_distance,
                          total_amount,
                          payment_type,
                          trip_type
                          FROM df_parquet
                          ''')

df_join_table = sqldf('''SELECT dpc.*,
                      dtz.Borough,
                      dtz.Zone,
                      dtz.service_zone 
                      FROM df_parquet_column dpc
                      INNER JOIN df_taxi_zonelookup dtz
                      ON dpc.PULocationID = dtz.LocationID
                      ''')

#jumlah record yang akan di save ke table
count_data = df_join_table.shape[0]

# Load Data Result After Do Transformation Process (Join 2 table become 1 table) to PostgreSQL table
TABLE_NAME = 'green_tripdata_zone_lookup1'
df_join_table.to_sql(name=TABLE_NAME, con=engine, index=False,if_exists='replace')
print(f'Total Record has been inserted are {count_data} to table {TABLE_NAME} ')