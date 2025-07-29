import pandas as pd
from pandasql import sqldf
from sqlalchemy import create_engine

# Konfigurasi database
db_uri = 'postgresql+psycopg2://postgres:postgres@host.docker.internal:5432/demo_intro_sql'
#f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

# Buat engine SQLAlchemy
engine = create_engine(db_uri)

#read data csv files
df_covid_all = pd.read_csv('/opt/airflow/dags/dataset/owid-covid-data.csv')

#transform process
#filter data total_case is not null
df_covid = sqldf(''' select * from df_covid_all where total_cases is not null  ''')

#jumlah record yang akan di save ke table
count_data = df_covid.shape[0]

#load data after transformation to postgresql
TABLE_NAME = 'covid_all_data'
with engine.connect() as connection:
    df_covid.to_sql(name=TABLE_NAME, con=connection,index=False,if_exists='replace')
    print(f'Total Record has been inserted are {count_data} to table {TABLE_NAME} ')
print("Koneksi database telah ditutup.")