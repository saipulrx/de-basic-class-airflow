import pandas as pd
import psycopg2
from psycopg2 import extras
import os
import json # Import modul json

def run_etl(config_path):
    """
    Melakukan proses ETL dari file CSV ke database PostgreSQL menggunakan konfigurasi dari file.
    """
    try:
        # Baca konfigurasi dari file JSON
        print(f"Membaca konfigurasi dari file: {config_path}")
        with open(config_path, 'r') as f:
            config = json.load(f)

        csv_file_path = os.path.join(os.path.dirname(config_path), config['csv_file']) # Gabungkan path dasar dengan path CSV
        db_host = config['db_host']
        db_name = config['db_name']
        db_user = config['db_user']
        db_password = config['db_password']
        db_port = config['db_port']
        table_name = config['table_name']

        # 1. Extract: Baca data dari file CSV
        print(f"Membaca data dari file CSV: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        print(f"Berhasil membaca {len(df)} baris dari CSV.")

        # 2. Transform: (Opsional) Lakukan transformasi data di sini
        # df.columns = [col.lower().replace(' ', '_') for col in df.columns]

        # 3. Load: Muat data ke database PostgreSQL
        conn = None
        cur = None
        try:
            print(f"Menghubungkan ke database PostgreSQL di {db_host}:{db_port}/{db_name}...")
            conn = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_password,
                port=db_port
            )
            cur = conn.cursor()
            print("Berhasil terhubung ke database.")

            columns = ', '.join(df.columns)
            values_placeholder = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"

            data_to_insert = [tuple(row) for row in df.values]

            print(f"Memasukkan {len(data_to_insert)} baris ke tabel {table_name}...")
            extras.execute_batch(cur, insert_query, data_to_insert)
            conn.commit()
            print("Data berhasil dimasukkan ke database.")

        except (Exception, psycopg2.Error) as error:
            print(f"Error saat terhubung atau berinteraksi dengan PostgreSQL: {error}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
                print("Koneksi PostgreSQL ditutup.")

    except FileNotFoundError:
        print(f"Error: File CSV atau config.json tidak ditemukan. Periksa path: {csv_file_path} atau {config_path}")
        raise
    except json.JSONDecodeError:
        print(f"Error: Gagal mem-parsing file JSON di {config_path}. Pastikan formatnya benar.")
        raise
    except KeyError as e:
        print(f"Error: Kunci '{e}' tidak ditemukan di file konfigurasi. Pastikan semua parameter ada.")
        raise
    except Exception as e:
        print(f"Terjadi kesalahan umum: {e}")
        raise

if __name__ == "__main__":
    # Ini adalah contoh penggunaan saat skrip dijalankan secara independen
    # Asumsikan config.json berada di direktori yang sama atau di 'data'
    CURRENT_DIR = os.path.dirname(__file__)
    CONFIG_FILE = os.path.join(CURRENT_DIR, 'config.json') # Sesuaikan jika config.json di tempat lain

    # Buat file sample.csv dan direktori 'data' jika belum ada untuk pengujian
    data_dir = os.path.join(CURRENT_DIR, 'data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    sample_csv_path = os.path.join(data_dir, 'sample.csv')
    if not os.path.exists(sample_csv_path):
        with open(sample_csv_path, 'w') as f:
            f.write("id,name,value\n")
            f.write("1,Alice,100\n")
            f.write("2,Bob,150\n")
            f.write("3,Charlie,200\n")
        print(f"File '{sample_csv_path}' telah dibuat sebagai contoh.")
    
    # Buat file config.json jika belum ada (untuk pengujian)
    if not os.path.exists(CONFIG_FILE):
        sample_config = {
            "csv_file": "data/sample.csv",
            "db_host": "localhost",
            "db_name": "mydatabase",
            "db_user": "myuser",
            "db_password": "mypassword",
            "db_port": 5432,
            "table_name": "my_table"
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(sample_config, f, indent=4)
        print(f"File '{CONFIG_FILE}' telah dibuat sebagai contoh.")


    try:
        run_etl(CONFIG_FILE)
    except Exception as e:
        print(f"Proses ETL gagal: {e}")