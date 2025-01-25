import pandas as pd
from pandasql import sqldf
from sqlalchemy import create_engine

# Konfigurasi database
db_uri = 'postgresql+psycopg2://postgres:12345@host.docker.internal:5432/de_mentor'
#f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

# Buat engine SQLAlchemy
engine = create_engine(db_uri)

# Do aggregate data From table and save to variable
df_aggregate = pd.read_sql(
    '''
    select 
    gtz."VendorID",
    case 
    	when gtz."VendorID" = 1 then 'Creative Mobile Technologies'
    	when gtz."VendorID" = 2 then 'VeriFone Inc.'
    end as vendor_name,
    count(gtz.passenger_count)passenger_count,
    sum(gtz.total_amount)sum_total_amount
    from green_tripdata_202107_zone_lookup gtz 
    where gtz."VendorID" in (1,2)
    group by gtz."VendorID"
    '''
,engine)

#jumlah record yang akan di save ke table
count_data = df_aggregate.shape[0]

# Load to New Table in Postgres
TABLE_NAME ='aggregate_green_tripdata_202107_zonelookup'
df_aggregate.to_sql(TABLE_NAME,engine,index=False,if_exists='replace')
print(f'Total Record has been inserted are {count_data} to table {TABLE_NAME} ')