import pandas as pd
from pandasql import sqldf
from sqlalchemy import create_engine

# Konfigurasi database
db_uri = 'postgresql+psycopg2://postgres:postgres@host.docker.internal:5432/demo_intro_sql'
#f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'

# Buat engine SQLAlchemy
engine = create_engine(db_uri)

df_covid = pd.read_sql(''' WITH LatestData AS (
                        SELECT
                            location,
                            total_cases,
                            gdp_per_capita,
                            date,
                            ROW_NUMBER() OVER(PARTITION BY location ORDER BY date DESC) as rn
                        FROM covid_all_data
                        WHERE location IS NOT NULL
                        AND total_cases IS NOT NULL
                        AND gdp_per_capita IS NOT NULL
                        )
                    SELECT
                        location,
                        total_cases,
                        gdp_per_capita
                    FROM LatestData
                    WHERE rn = 1
                    ORDER BY total_cases DESC  ''', engine)

#jumlah record yang akan di save ke table
count_data = df_covid.shape[0]

#load data after transformation to postgresql
TABLE_NAME = 'covid_total_pdp_perkapita'
with engine.connect() as connection:
    df_covid.to_sql(name=TABLE_NAME, con=connection,index=False,if_exists='replace')
    print(f'Total Record has been inserted are {count_data} to table {TABLE_NAME} ')
print("Koneksi database telah ditutup.")