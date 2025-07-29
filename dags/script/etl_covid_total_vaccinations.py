import pandas as pd
from pandasql import sqldf
from sqlalchemy import create_engine

# Konfigurasi database
db_uri = 'postgresql+psycopg2://postgres:postgres@host.docker.internal:5432/demo_intro_sql'
#f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

# Buat engine SQLAlchemy
engine = create_engine(db_uri)

df_covid = pd.read_sql(''' SELECT
                        date,
                        total_vaccinations
                    FROM covid_all_data
                    WHERE location = 'India' -- Ganti 'India' dengan negara yang Anda inginkan
                    AND total_vaccinations IS NOT NULL
                    ORDER BY date  ''', engine)

#jumlah record yang akan di save ke table
count_data = df_covid.shape[0]

#load data after transformation to postgresql
TABLE_NAME = 'covid_total_vaccinations_by_date'
with engine.connect() as connection:
    df_covid.to_sql(name=TABLE_NAME, con=connection,index=False,if_exists='replace')
    print(f'Total Record has been inserted are {count_data} to table {TABLE_NAME} ')
print("Koneksi database telah ditutup.")