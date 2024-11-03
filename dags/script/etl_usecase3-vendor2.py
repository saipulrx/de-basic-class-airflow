import pandas as pd
from pandasql import sqldf
from sqlalchemy import create_engine

# Konfigurasi database
db_uri = 'postgresql+psycopg2://postgres:12345@host.docker.internal:5432/de_mentor'
#f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

# Buat engine SQLAlchemy
engine = create_engine(db_uri)

df_aggregate = pd.read_sql(
    '''
    select 
    gtz."RatecodeID",
    case 
    	when gtz."RatecodeID" = 1 then 'Standard Rate'
    	when gtz."RatecodeID" = 2 then 'JFK'
        when gtz."RatecodeID" = 3 then 'Newark'
        when gtz."RatecodeID" = 4 then 'Nassau or Westchester'
        when gtz."RatecodeID" = 5 then 'Negotiated fare'
        when gtz."RatecodeID" = 6 then 'Group ride' 
    end as rate_code_name,
    count(gtz.passenger_count)passenger_count,
    sum(gtz.total_amount)sum_total_amount
    from green_tripdata_zone_lookup_vendor2 gtz 
    group by gtz."RatecodeID"
    order by gtz."RatecodeID"
    '''
,engine)

#jumlah record yang akan di save ke table
count_data = df_aggregate.shape[0]

# Load to New Table in Postgres
TABLE_NAME ='aggregate_green_tripdata_zone_lookup_vendor2'
df_aggregate.to_sql(TABLE_NAME,engine,index=False,if_exists='replace')
print(f'Total Record has been inserted are {count_data} to table {TABLE_NAME} ')