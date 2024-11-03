import pandas as pd
from pandasql import sqldf
from sqlalchemy import create_engine

# Konfigurasi database
db_uri = 'postgresql+psycopg2://postgres:12345@host.docker.internal:5432/de_mentor'

# Buat engine SQLAlchemy
engine = create_engine(db_uri)

#read data csv files
df_covid_all = pd.read_csv('/opt/airflow/dags/dataset/owid-covid-data.csv')

#transform process
#filter data total_case is not null
#and total_deaths is not null
#and location bahrain
df_bahrain = sqldf('''
select * from df_covid_all where location = "Bahrain" and total_cases IS NOT NULL and total_deaths IS NOT NULL
''')

count_data = df_bahrain.shape[0]
#load data after transformation to postgresql
TABLE_NAME = 'data_covid_bahrains'
df_bahrain.to_sql(name=TABLE_NAME, con=engine,index=False,if_exists='replace')
print(f'Total Record has been inserted are {count_data} to table {TABLE_NAME} ')